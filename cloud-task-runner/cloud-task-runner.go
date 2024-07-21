package main

import (
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
	"log"
	"os"
	"time"
)

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

	flag.Parse()

	checkRequiredFlags(dynamoDocApiEndpoint, s3Bucket, stagesConfigPath, taskId, taskDefinitionPath, runParametersFilePath, outputFile)

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
		log.Fatalf("failed starting the pipeline: %v", err)
	}
	log.Println("Submitted task run", newRunUUID.String(), "for task", *taskId)

	// TODO we should wait not only on finished-tasks but also check cancelled-tasks and failed ones
	finishedTaskRunID, err := registry.WaitForPipelineFinish(*taskId, newRunUUID.String())
	if err != nil {
		log.Fatalf("failed while waiting for the pipeline to finish: %v", err)
	}
	if finishedTaskRunID != taskRun.UUID {
		log.Fatalf("pipeline returned %s as finished task run but expected %s\n", finishedTaskRunID, taskRun.UUID)
	}
	log.Println("Pipeline finished successfully!")

	finishedTask, err := registry.GetTaskRun(finishedTaskRunID)
	if err != nil {
		log.Fatalf("failed getting task run information from DB: %v", err)
	}

	finishedStages, err := registry.GetAllStages(finishedTaskRunID)
	if err != nil {
		log.Fatalf("failed getting stages information from DB: %v", err)
	}

	printTaskReportWithAllStages(finishedTask, finishedStages)

	if allStagesHaveStatus(finishedStages, cloud_task_registry.StageStatus_Success) {
		err = printMapToFile(*outputFile, finishedTask.Results)
		if err != nil {
			log.Fatalf("failed printing results into the output file: %v", err)
		}
		fmt.Println("Written output to", *outputFile)
		os.Exit(0)
	} else {
		os.Exit(-1)
	}
}

func checkRequiredFlags(
	dynamoDocApiEndpoint *string,
	s3Bucket *string,
	stagesConfigPath *string,
	taskName *string,
	taskDefinitionPath *string,
	runParametersFilePath *string,
	outputFile *string,
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
	if *taskName == "" {
		log.Fatal("Please provide --task-name")
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
}

func convertUuidTime(t uuid.Time) time.Time {
	sec, nsec := t.UnixTime()
	return time.Unix(sec, nsec).UTC()
}

func printMapToFile(filename string, m map[string]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %q: %w", filename, err)
	}
	defer file.Close()

	for key, value := range m {
		fmt.Printf("%s %s\n", value, key)
		_, err := fmt.Fprintf(file, "%s %s\n", value, key)
		if err != nil {
			return fmt.Errorf("failed to write to file %q: %w", filename, err)
		}
	}

	return nil
}
