package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	cloud_task_registry "github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
)

const pidsFile = "cloud-task-runner.pids"

func main() {
	dynamoDocApiEndpoint :=
		flag.String("dynamo-docapi-endpoint", "", "DynamoDB Document API endpoint URL for task registry")
	s3Bucket :=
		flag.String("s3-bucket", "", "S3 bucket name to use for task registry")
	stagesConfigPath :=
		flag.String("stages-config-file", "stages.yaml", "YAML file with pipeline stages configuration")
	taskId :=
		flag.String("task-id", "", "Optimization task ID or name (use only symbols supported by S3)")
	taskDefinitionPath :=
		flag.String("task-definition-file", "optimization.in", "File with the optimization task configuration")
	runParametersFilePath :=
		flag.String("parameters-file", "params.in", "File with optimization parameters for the task run, in 'k=v' per line format")
	outputFile :=
		flag.String("output-file", "", "File where to write the calculated objective function(s) value(s)")
	dlqName :=
		flag.String("dlq-name", "DLQ", "Name of the Dead Letter Queue to monitor for failed tasks")
	objectivesArg :=
		flag.String("objectives", "", "Comma-separated list of required objectives names (e.g. 'obj1,obj2')")
	missingObjectiveValue :=
		flag.String("missing-obj-value", "NaN", "Value to put as objectives if they are not present in task results (e.g., due to task failure)")

	flag.Parse()

	checkRequiredFlags(dynamoDocApiEndpoint, s3Bucket, stagesConfigPath, taskId, taskDefinitionPath, runParametersFilePath, outputFile, objectivesArg)
	objectives := parseObjectivesArg(*objectivesArg)

	dumpProcessId()

	newRunUUID, err := uuid.NewV7()
	if err != nil {
		log.Fatalf("Error creating UUID: %v", err)
	}

	taskParameters, err := readKeyValueFile(*runParametersFilePath)
	if err != nil {
		log.Fatalf("Error reading key-value file: %v", err)
	}

	_, err = os.Stat(*taskDefinitionPath)
	if err != nil {
		log.Fatalf("Cannot stat task definition file: %v", err)
	}

	registry, err := cloud_task_registry.New(*dynamoDocApiEndpoint)
	if err != nil {
		log.Fatalf("Error connection to the Cloud Task Registry, %v", err)
	}

	//fetchedStage, err := registry.GetStage("019090c8-68d9-7823-8f5d-0e6649c759ea", 4)
	//if err != nil {
	//	log.Fatalf("failed to get taskRun: %v", err)
	//}
	//log.Printf("fetched stage: %+v\n", fetchedStage)
	//
	//fetchedStage, err = registry.GetNextStage(fetchedStage)
	//if err != nil {
	//	log.Fatalf("failed to get taskRun: %v", err)
	//}
	//log.Printf("fetched stage: %+v\n", fetchedStage)
	//
	//os.Exit(0)

	s3Path, err := registry.UploadFileForTask(*taskDefinitionPath, *s3Bucket, *taskId, newRunUUID.String())
	if err != nil {
		log.Fatalf("Error uploading task defition file to S3, %v", err)
	}

	taskCreationTime := convertUuidTime(newRunUUID.Time())
	taskRun := cloud_task_registry.TaskRun{
		TaskID:         *taskId,
		UUID:           newRunUUID.String(),
		Parameters:     taskParameters,
		Results:        nil,
		TaskDefinition: s3Path,
		CreationTime:   &taskCreationTime,
		Status:         cloud_task_registry.TaskRunStatus_Submitted,
	}

	stages, err := createStages(registry, &taskRun, *stagesConfigPath, *s3Bucket)
	if err != nil {
		log.Fatalf("Error reading stages config file: %v", err)
	}

	if err := registry.InsertTaskRun(taskRun); err != nil {
		log.Fatalf("failed to insert task run: %v", err)
	}
	log.Println("Successfully inserted task run with id", newRunUUID.String(), "for task", *taskId)

	for _, stage := range stages {
		if err := registry.InsertStage(stage); err == nil {
			log.Println("Successfully inserted stage", stage.NOrd, "(", stage.Name, ")")
		} else {
			log.Fatalf("failed to insert stage: %v", err)
		}
	}

	err = registry.PassTaskToStage(&stages[0])
	if err != nil {
		_ = registry.UpdateTaskRunStatus(&taskRun, cloud_task_registry.TaskRunStatus_Failed)
		log.Fatalf("failed starting the pipeline: %v", err)
	}
	log.Println("Submitted task run", newRunUUID.String(), "for task", *taskId)

	wasCancelled := make(chan bool, 3)
	setupCancellationHandler(registry, &taskRun, wasCancelled)

	// Start waiting for both normal queue and DLQ
	finishedTaskRunIDChan := make(chan string, 1)
	dlqTaskRunIDChan := make(chan string, 1)
	waitErrChan := make(chan error, 2)

	go func() {
		id, err := registry.WaitForPipelineFinish(*taskId, newRunUUID.String(), wasCancelled)
		if err != nil {
			waitErrChan <- err
		} else {
			finishedTaskRunIDChan <- id
		}
	}()

	go func() {
		id, err := registry.WaitForDLQ(*dlqName, newRunUUID.String(), wasCancelled)
		if err != nil {
			waitErrChan <- err
		} else {
			dlqTaskRunIDChan <- id
		}
	}()

	cancelled := <-wasCancelled
	if cancelled {
		log.Printf("%s (run %s): Task execution cancelled!\n", taskRun.TaskID, taskRun.UUID)
		os.Exit(-1)
	}

	var finishedTaskRunID string
	var dlqTriggered bool

	select {
	case err := <-waitErrChan:
		log.Fatalf("failed while waiting for the pipeline to finish: %v", err)
	case finishedTaskRunID = <-finishedTaskRunIDChan:
		if finishedTaskRunID != taskRun.UUID {
			log.Fatalf("pipeline returned %s as finished task run but expected %s\n", finishedTaskRunID, taskRun.UUID)
		}
		log.Println("Pipeline finished successfully!")
	case dlqID := <-dlqTaskRunIDChan:
		if dlqID == taskRun.UUID {
			log.Println("Task run", dlqID, "was found in DLQ, marking as failed.")
			dlqTriggered = true
		} else {
			log.Fatalf("DLQ returned %s as finished task run but expected %s\n", dlqID, taskRun.UUID)
		}
	}

	finishedTask, err := registry.GetTaskRun(taskRun.UUID)
	if err != nil {
		log.Fatalf("failed getting task run information from DB: %v", err)
	}

	finishedStages, err := registry.GetAllStages(taskRun.UUID)
	if err != nil {
		log.Fatalf("failed getting stages information from DB: %v", err)
	}

	printTaskReportWithAllStages(finishedTask, finishedStages)

	if dlqTriggered {
		// Mark as failed and write -1 for missing objectives
		if err := registry.UpdateTaskRunStatus(&taskRun, cloud_task_registry.TaskRunStatus_Failed); err != nil {
			log.Println("Failed setting status", cloud_task_registry.TaskRunStatus_Failed,
				"to task run", taskRun.UUID, "of task", taskRun.TaskID, "(non-critical error)", err)
		}
		err = printResultsToFile(*outputFile, objectives, finishedTask.Results, *missingObjectiveValue)
		if err != nil {
			log.Fatalf("failed printing results into the output file: %v", err)
		}
		fmt.Println("Written output to", *outputFile)
		os.Exit(0)
	} else {
		if allStagesHaveStatus(finishedStages, cloud_task_registry.StageStatus_Success) {
			err = printResultsToFile(*outputFile, objectives, finishedTask.Results, *missingObjectiveValue)
			if err != nil {
				log.Fatalf("failed printing results into the output file: %v", err)
			}
			fmt.Println("Written output to", *outputFile)
			err = registry.UpdateTaskRunStatus(&taskRun, cloud_task_registry.TaskRunStatus_Finished)
			if err != nil {
				log.Println("Failed setting status", cloud_task_registry.TaskRunStatus_Finished,
					"to task run", taskRun.UUID, "of task", taskRun.TaskID, "(non-critical error)", err)
			}
			os.Exit(0)
		} else {
			// Unlikely situation: pipeline finished with erroneous stage(s) but via finished-tasks queue implying success
			// TODO iterate over the result and put NaNs to the missing ones (?)
			if anyStageHasStatus(finishedStages, cloud_task_registry.StageStatus_Error) {
				err = registry.UpdateTaskRunStatus(&taskRun, cloud_task_registry.TaskRunStatus_Failed)
				if err != nil {
					log.Println("Failed setting status", cloud_task_registry.TaskRunStatus_Failed,
						"to task run", taskRun.UUID, "of task", taskRun.TaskID, "(non-critical error)", err)
				}
			}
			os.Exit(-1)
		}
	}
}

func dumpProcessId() {
	pid := os.Getpid()
	f, err := os.OpenFile(pidsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatalf("Could not close file %q: %v", pidsFile, err)
		}
	}(f)
	if err != nil {
		log.Fatalf("Couldn't open %q: %v", pidsFile, err)
	}
	w := bufio.NewWriter(f)
	_, err = w.WriteString(fmt.Sprintf("%d ", pid))
	if err != nil {
		log.Fatalf("Couldn't write PID to file %q: %v", pidsFile, err)
	}
	err = w.Flush()
	if err != nil {
		log.Fatalf("Couldn't flush PID to file %q: %v", pidsFile, err)
	}
	log.Println("The cloud task runner's PID", pid, "appended to", pidsFile)
}

func checkRequiredFlags(
	dynamoDocApiEndpoint *string,
	s3Bucket *string,
	stagesConfigPath *string,
	taskId *string,
	taskDefinitionPath *string,
	runParametersFilePath *string,
	outputFile *string,
	objectivesList *string,
) {
	if *dynamoDocApiEndpoint == "" {
		log.Fatal("Please provide --dynamo-docapi-endpoint")
	}
	if *s3Bucket == "" {
		log.Fatal("Please provide --s3-bucket")
	}
	if *stagesConfigPath == "" {
		log.Fatal("Please provide --stages-config-file")
	}
	if *taskId == "" {
		log.Fatal("Please provide --task-id")
	}
	if *taskDefinitionPath == "" {
		log.Fatal("Please provide --task-definition-file")
	}
	if *runParametersFilePath == "" {
		log.Fatal("Please provide --parameters-file")
	}
	if *outputFile == "" {
		log.Fatal("Please provide --output-file")
	}
	if *objectivesList == "" {
		log.Fatal("Please provide --objectives (comma-separated list of required objectives names)")
	}
}

func convertUuidTime(t uuid.Time) time.Time {
	sec, nsec := t.UnixTime()
	return time.Unix(sec, nsec).UTC()
}

func printResultsToFile(filename string, objectives []string, results map[string]string, missingObjValue string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %q: %w", filename, err)
	}
	defer file.Close()

	for _, obj := range objectives {
		if value, ok := results[obj]; !ok {
			fmt.Printf("%s %s\n", missingObjValue, obj)
			_, err = fmt.Fprintf(file, "%s %s\n", missingObjValue, obj)
		} else {
			fmt.Printf("%s %s\n", value, obj)
			_, err = fmt.Fprintf(file, "%s %s\n", value, obj)
		}
		if err != nil {
			return fmt.Errorf("failed to write to file %q: %w", filename, err)
		}
	}

	return nil
}

func setupCancellationHandler(
	registry *cloud_task_registry.CloudTaskRegistry,
	taskRun *cloud_task_registry.TaskRun,
	wasCancelled chan bool,
) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("Got interrupt, cancelling the task...")
		err := registry.UpdateTaskRunStatus(taskRun, cloud_task_registry.TaskRunStatus_Cancelled)
		if err != nil {
			log.Println("Failed to cancel task:", err)
		} else {
			wasCancelled <- true
		}
	}()
}

func parseObjectivesArg(arg string) []string {
	var objs []string
	for _, obj := range splitAndTrim(arg, ",") {
		if obj != "" {
			objs = append(objs, obj)
		}
	}
	return objs
}

func splitAndTrim(s, sep string) []string {
	var res []string
	for _, part := range split(s, sep) {
		res = append(res, trim(part))
	}
	return res
}

func split(s, sep string) []string {
	return strings.Split(s, sep)
}

func trim(s string) string {
	return strings.TrimSpace(s)
}
