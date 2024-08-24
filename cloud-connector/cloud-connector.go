package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

type AppError struct {
	Error   error
	Message string
	Code    int
	Stage   *cloud_task_registry.Stage
}

var taskRegistry *cloud_task_registry.CloudTaskRegistry

func main() {
	if os.Getenv("SHOW_CPU_INFO") != "" {
		logCpuInformation()
	}
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("PORT environment variable not set")
	}

	pipelineStage := flag.String("pipeline-stage", "", "Stage of the pipeline")
	configFilePath := flag.String("config-file-path", "/tmp/config", "Path to the config file (internal)")
	inputFilePath := flag.String("input-file-path", "/tmp/input", "Path to the input file (internal)")
	outputFilePath := flag.String("output-file-path", "/tmp/output", "Path to the output file (internal)")
	commandFilePath := flag.String("command-file-path", "/tmp/run-command.sh", "Path to the command file (internal)")
	dynamoDocApiEndpoint := flag.String("dynamo-docapi-endpoint", "", "DynamoDB Document API endpoint URL for task registry")
	extraArtifacts := flag.String("extra-artifacts", "", "Comma-delimited paths to extra artifacts (files and/or folders) to upload to S3")
	maxTimeForExtrasArchiving := flag.Int("max-archiving-time", 90, "Max time that is expected to be spent on archiving extra artifacts")
	timeout := flag.Int("timeout", 600, "Request processing timeout (in seconds) that is imposed by the cloud execution environment")
	// If there is less than [maxTimeForExtrasArchiving] seconds before [timout], extra artifacts will not be compressed and uploaded to S3

	flag.Parse()

	// Check for required flags
	if *pipelineStage == "" {
		log.Fatal("--pipeline-stage arg is mandatory, this must be the name of this stage")
	}
	if *commandFilePath == "" {
		log.Fatal("--command-file-path arg is mandatory, this must be the path to the command to execute")
	}
	if *dynamoDocApiEndpoint == "" {
		log.Fatal("--dynamo-docapi-endpoint arg is mandatory, this must be DynamoDB Document API endpoint URL for task registry")
	}

	if registry, err := cloud_task_registry.New(*dynamoDocApiEndpoint); err != nil {
		log.Fatalf("Could not connect to the Cloud Task Registry: %s\n", err.Error())
	} else {
		log.Println("Connected to the Cloud Task Registry", *dynamoDocApiEndpoint)
		taskRegistry = registry
	}

	extraArtifactsPaths := strings.Split(*extraArtifacts, ",")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		timeoutRisk := false
		timer := time.AfterFunc(time.Duration(*timeout-*maxTimeForExtrasArchiving)*time.Second, func() { timeoutRisk = true })
		appErr := handler(w, r, *pipelineStage, *configFilePath, *inputFilePath, *outputFilePath, *commandFilePath, extraArtifactsPaths, &timeoutRisk)
		if appErr != nil {
			log.Printf("Error: %s (%v)", appErr.Message, appErr.Error)
			http.Error(w, appErr.Message, appErr.Code)
			if appErr.Stage != nil {
				log.Println("Setting stage status to", cloud_task_registry.StageStatus_Error)
				if err := taskRegistry.UpdateStageStatus(appErr.Stage, cloud_task_registry.StageStatus_Error); err != nil {
					log.Printf("Error updating stage status: %v", err)
				}
			}
		}
		timer.Stop()
	})

	log.Println("Starting server on port", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Could not start server: %s\n", err.Error())
	}
}

func handler(
	w http.ResponseWriter,
	r *http.Request,
	pipelineStage, configPath, inputFilePath, outputFilePath, commandFilePath string,
	extraArtifactsPaths []string,
	timeoutRisk *bool,
) *AppError {
	taskId, appErr := extractSQSMessageBodyFromYandexCloudTriggerRequest(r)
	if appErr != nil {
		return appErr
	}

	if taskId == "" {
		msg := fmt.Sprintf("Expected task run UUID in SQS message body but it was empty! %v", r)
		return &AppError{errors.New(msg), msg, http.StatusBadRequest, nil}
	}

	stage, errGetStage := taskRegistry.GetStageByName(taskId, pipelineStage)
	if errGetStage != nil {
		msg := fmt.Sprintf("Unable to get stage %s for task %s", pipelineStage, taskId)
		return &AppError{errGetStage, msg, http.StatusInternalServerError, stage}
	}

	taskRun, errGetTask := taskRegistry.GetTaskRun(stage.TaskRunUUID)
	if errGetTask != nil {
		msg := fmt.Sprintf("couldn't get task run %s from the task registry", stage.TaskRunUUID)
		return &AppError{errGetTask, msg, http.StatusInternalServerError, stage}
	}

	taskWasCancelled, err := taskRegistry.IsCancelled(taskRun.UUID)
	if taskWasCancelled {
		markAsCancelled(stage)
		return nil
	}
	if err != nil {
		log.Println("Couldn't check if task run is cancelled. Assuming it is not... The error was", err)
	}

	if err := startStage(stage); err != nil {
		return err
	}

	if err := downloadConfigFileIfSpecified(stage, configPath); err != nil {
		return err
	}

	if err := downloadInputFileIfSpecified(stage, inputFilePath); err != nil {
		return err
	}

	if err := startCommandAndWait(commandFilePath, stage, taskRun.Parameters); err != nil {
		return err
	}

	if taskWasCancelled, _ := taskRegistry.IsCancelled(taskRun.UUID); taskWasCancelled {
		log.Println("Setting cancellation status to the stage", stage.Name, "for task run", stage.TaskRunUUID)
		err := taskRegistry.UpdateStageStatus(stage, cloud_task_registry.StageStatus_Cancelled)
		if err != nil {
			log.Println("Unable to update status for stage", stage.Name, "for task run",
				stage.TaskRunUUID, "(non-critical error)", err)
		}
	} else {
		s3PathForOutput, appErr := uploadOutputFile(outputFilePath, taskRun, stage)
		if appErr != nil {
			return appErr
		}

		if err := handoverTask(stage, taskRun, s3PathForOutput, outputFilePath); err != nil {
			return err
		}

		if err := finishStage(stage, taskRun); err != nil {
			return err
		}

		if len(extraArtifactsPaths) > 0 && extraArtifactsPaths[0] != "" {
			if !*timeoutRisk {
				uploadExtraArtifactsAndUpdateStageComment(extraArtifactsPaths, taskRun, stage)
			} else {
				log.Println("Extra artifacts will not be uploaded due to timeout risk!")
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Success"))
	return nil
}

func markAsCancelled(stage *cloud_task_registry.Stage) {
	log.Println("Setting cancelled status to the stage", stage.Name, "for task run", stage.TaskRunUUID)
	err := taskRegistry.UpdateStageStatus(stage, cloud_task_registry.StageStatus_Cancelled)
	if err != nil {
		log.Println("Unable to update status for stage", stage.Name, "for task run",
			stage.TaskRunUUID, "(non-critical error)", err)
	}
}

func startStage(stage *cloud_task_registry.Stage) *AppError {
	if err := taskRegistry.UpdateStageStatus(stage, cloud_task_registry.StageStatus_InProgress); err != nil {
		msg := fmt.Sprintf("Unable to update status for stage %s for task %s", stage.Name, stage.TaskRunUUID)
		return &AppError{err, msg, http.StatusInternalServerError, stage}
	}
	if err := taskRegistry.UpdateStageStartTime(stage, time.Now().UTC()); err != nil {
		msg := fmt.Sprintf("Unable to update start time for stage %s for task %s", stage.Name, stage.TaskRunUUID)
		return &AppError{err, msg, http.StatusInternalServerError, stage}
	}
	return nil
}

func finishStage(stage *cloud_task_registry.Stage, task *cloud_task_registry.TaskRun) *AppError {
	if err := taskRegistry.UpdateStageStatus(stage, cloud_task_registry.StageStatus_Success); err != nil {
		msg := fmt.Sprintf("error setting successful status to this stage, task %s", task.UUID)
		return &AppError{err, msg, http.StatusInternalServerError, stage}
	}

	if err := taskRegistry.UpdateStageFinishTime(stage, time.Now().UTC()); err != nil {
		msg := fmt.Sprintf("Unable to update finish time for stage %s for task %s", stage.Name, stage.TaskRunUUID)
		return &AppError{err, msg, http.StatusInternalServerError, stage}
	}
	return nil
}

func handoverTask(
	stage *cloud_task_registry.Stage,
	taskRun *cloud_task_registry.TaskRun,
	s3PathForOutput string,
	outputFilePath string,
) *AppError {
	if len(stage.Next) > 0 {
		for _, nextStageName := range stage.Next {
			nextStage, errGetNextStage := taskRegistry.GetStageByName(stage.TaskRunUUID, nextStageName)
			if errGetNextStage != nil {
				msg := "error getting next stage"
				return &AppError{errGetNextStage, msg, http.StatusInternalServerError, stage}
			}
			if nextStage.Status != cloud_task_registry.StageStatus_Pending {
				log.Println("The next stage", nextStage.Name, "is not pending! This probably "+
					"means that this stage was interrupted by timeout after it passed the task to "+
					"the next one(s). To avoid task duplication in SQS, the connector will not "+
					"pass it further. Task run ID was", taskRun.UUID, "for task", taskRun.TaskID)
			}
			if s3PathForOutput != "" {
				if err := taskRegistry.UpdateStageInput(nextStage, s3PathForOutput); err != nil {
					msg := fmt.Sprintf("error setting input for the next stage %v", nextStage)
					return &AppError{err, msg, http.StatusInternalServerError, stage}
				}
			} else {
				log.Println("No output file was uploaded to S3, so input for the next stage will be absent!")
			}
			if err := taskRegistry.PassTaskToStage(nextStage); err != nil {
				msg := fmt.Sprintf("error passing task to the next stage %v", nextStage)
				return &AppError{err, msg, http.StatusInternalServerError, stage}
			}
		}
	} else {
		log.Println("This stage is final in the task pipeline. Reading results...")
		// output file is required to be in format "k=v" per line where k is an objective name
		resultsMap, errReadResults := readKeyValueFile(outputFilePath)
		if errReadResults != nil {
			msg := "error reading output files to get results"
			return &AppError{errReadResults, msg, http.StatusInternalServerError, stage}
		} else {
			log.Printf("Read results: %v\n", resultsMap)
		}
		if err := taskRegistry.PutTaskRunResults(taskRun, resultsMap); err != nil {
			msg := fmt.Sprintf("error setting results for the task run %s", taskRun.UUID)
			return &AppError{err, msg, http.StatusInternalServerError, stage}
		}
		if err := taskRegistry.FinishTaskRun(taskRun.UUID); err != nil {
			msg := fmt.Sprintf("error finishing the task run %s", taskRun.UUID)
			return &AppError{err, msg, http.StatusInternalServerError, stage}
		}
	}
	return nil
}

func logCpuInformation() {
	cmd := exec.Command("lscpu")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		log.Println("Failed to call lscpu due to error:", err)
	}
}
