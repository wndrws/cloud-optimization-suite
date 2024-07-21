package cloud_task_registry

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (registry *CloudTaskRegistry) InsertTaskRun(task TaskRun) error {
	av, err := attributevalue.MarshalMap(task)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(TasksTable),
		Item:      av,
	}

	_, err = registry.dynamodbClient.PutItem(context.TODO(), input)
	return err
}

func (registry *CloudTaskRegistry) InsertStage(stage Stage) error {
	av, err := attributevalue.MarshalMap(stage)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(StagesTable),
		Item:      av,
	}

	_, err = registry.dynamodbClient.PutItem(context.TODO(), input)
	return err
}

// TODO make this work, as well
//func (registry *CloudTaskRegistry) GetTask(taskId string) (*TaskRun, error) {
//	input := &dynamodb.GetItemInput{
//		TableName: aws.String(TasksTable),
//		Key: map[string]types.AttributeValue{
//			"task_id": &types.AttributeValueMemberS{Value: taskId},
//		},
//	}
//
//	result, err := registry.dynamodbClient.GetItem(context.TODO(), input)
//	if err != nil {
//		return nil, err
//	}
//
//	var task TaskRun
//	err = attributevalue.UnmarshalMap(result.Item, &task)
//	if err != nil {
//		return nil, err
//	}
//
//	return &task, nil
//}

func (registry *CloudTaskRegistry) GetTaskRun(taskRunUUID string) (*TaskRun, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(TasksTable),
		IndexName:              aws.String("TaskRunUUIDIndex"),
		KeyConditionExpression: aws.String("run_uuid = :run_uuid"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":run_uuid": &types.AttributeValueMemberS{Value: taskRunUUID},
		},
	}

	result, err := registry.dynamodbClient.Query(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	if len(result.Items) == 0 {
		return nil, fmt.Errorf("task run '%s' not found", taskRunUUID)
	}
	if len(result.Items) > 1 {
		return nil, fmt.Errorf("task run '%s' is not unique", taskRunUUID)
	}

	var task TaskRun
	err = attributevalue.UnmarshalMap(result.Items[0], &task)
	if err != nil {
		return nil, err
	}

	return &task, nil
}

// PutTaskRunResults TODO support append?
func (registry *CloudTaskRegistry) PutTaskRunResults(taskRun *TaskRun, results map[string]string) error {
	av, err := attributevalue.MarshalMap(results)
	if err != nil {
		return err
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(TasksTable),
		Key: map[string]types.AttributeValue{
			"task_id":  &types.AttributeValueMemberS{Value: taskRun.TaskID},
			"run_uuid": &types.AttributeValueMemberS{Value: taskRun.UUID},
		},
		UpdateExpression:    aws.String("SET #results = :results"),
		ConditionExpression: aws.String("attribute_exists(run_uuid)"),
		ExpressionAttributeNames: map[string]string{
			"#results": "results",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":results": &types.AttributeValueMemberM{Value: av},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}

	_, err = registry.dynamodbClient.UpdateItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to update task run results: %w", err)
	}

	return nil
}

func (registry *CloudTaskRegistry) GetStage(taskRunUUID string, nOrd int) (*Stage, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(StagesTable),
		Key: map[string]types.AttributeValue{
			"run_uuid": &types.AttributeValueMemberS{Value: taskRunUUID},
			"n_ord":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", nOrd)},
		},
	}

	result, err := registry.dynamodbClient.GetItem(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	if len(result.Item) == 0 {
		return nil, nil
	}

	var stage Stage
	err = attributevalue.UnmarshalMap(result.Item, &stage)
	if err != nil {
		return nil, err
	}

	return &stage, nil
}

func (registry *CloudTaskRegistry) GetStageByName(taskRunUUID, stageName string) (*Stage, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(StagesTable),
		IndexName:              aws.String("StageNameIndex"),
		KeyConditionExpression: aws.String("run_uuid = :run_uuid and name = :name"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":run_uuid": &types.AttributeValueMemberS{Value: taskRunUUID},
			":name":     &types.AttributeValueMemberS{Value: stageName},
		},
	}

	result, err := registry.dynamodbClient.Query(context.TODO(), input)
	if err != nil {
		return nil, err
	}
	if len(result.Items) == 0 {
		return nil, fmt.Errorf("stage '%s' not found in task %s", stageName, taskRunUUID)
	}
	if len(result.Items) > 1 {
		return nil, fmt.Errorf("stage '%s' is not unique in task %s", stageName, taskRunUUID)
	}

	var stage Stage
	err = attributevalue.UnmarshalMap(result.Items[0], &stage)
	if err != nil {
		return nil, err
	}

	return &stage, nil
}

func (registry *CloudTaskRegistry) GetAllStages(taskRunUUID string) ([]Stage, error) {
	result, err := registry.dynamodbClient.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String(StagesTable),
		KeyConditionExpression: aws.String("run_uuid = :run_uuid"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":run_uuid": &types.AttributeValueMemberS{Value: taskRunUUID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query stages: %w", err)
	}

	var stages []Stage
	err = attributevalue.UnmarshalListOfMaps(result.Items, &stages)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal stages: %w", err)
	}

	return stages, nil
}

func (registry *CloudTaskRegistry) UpdateStageStatus(stage *Stage, newStatus string) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(StagesTable),
		Key: map[string]types.AttributeValue{
			"run_uuid": &types.AttributeValueMemberS{Value: stage.TaskRunUUID},
			"n_ord":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", stage.NOrd)},
		},
		UpdateExpression:    aws.String("SET #status = :newStatus"),
		ConditionExpression: aws.String("attribute_exists(n_ord)"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newStatus": &types.AttributeValueMemberS{Value: newStatus},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}

	_, err := registry.dynamodbClient.UpdateItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to update stage status: %w", err)
	}

	return nil
}

func (registry *CloudTaskRegistry) UpdateStageOutput(stage *Stage, path string) error {
	updateItem := &dynamodb.UpdateItemInput{
		TableName: aws.String(StagesTable),
		Key: map[string]types.AttributeValue{
			"run_uuid": &types.AttributeValueMemberS{Value: stage.TaskRunUUID},
			"n_ord":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", stage.NOrd)},
		},
		UpdateExpression:    aws.String("SET #output = :path"),
		ConditionExpression: aws.String("attribute_exists(n_ord)"),
		ExpressionAttributeNames: map[string]string{
			"#output": "output",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":path": &types.AttributeValueMemberS{Value: path},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}

	_, err := registry.dynamodbClient.UpdateItem(context.TODO(), updateItem)
	if err != nil {
		return fmt.Errorf("failed to update stage output: %w", err)
	}

	return nil
}

func (registry *CloudTaskRegistry) UpdateStageInput(stage *Stage, path string) error {
	updateItem := &dynamodb.UpdateItemInput{
		TableName: aws.String(StagesTable),
		Key: map[string]types.AttributeValue{
			"run_uuid": &types.AttributeValueMemberS{Value: stage.TaskRunUUID},
			"n_ord":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", stage.NOrd)},
		},
		UpdateExpression:    aws.String("SET #input = :path"),
		ConditionExpression: aws.String("attribute_exists(n_ord)"),
		ExpressionAttributeNames: map[string]string{
			"#input": "input",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":path": &types.AttributeValueMemberS{Value: path},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}

	_, err := registry.dynamodbClient.UpdateItem(context.TODO(), updateItem)
	if err != nil {
		return fmt.Errorf("failed to update stage input: %w", err)
	} // TODO check if update was done?

	return nil
}

func (registry *CloudTaskRegistry) UpdateStageComment(stage *Stage, comment string) error {
	updateItem := &dynamodb.UpdateItemInput{
		TableName: aws.String(StagesTable),
		Key: map[string]types.AttributeValue{
			"run_uuid": &types.AttributeValueMemberS{Value: stage.TaskRunUUID},
			"n_ord":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", stage.NOrd)},
		},
		UpdateExpression:    aws.String("SET #comment = :comment"),
		ConditionExpression: aws.String("attribute_exists(n_ord)"),
		ExpressionAttributeNames: map[string]string{
			"#comment": "comment",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":comment": &types.AttributeValueMemberS{Value: comment},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}

	_, err := registry.dynamodbClient.UpdateItem(context.TODO(), updateItem)
	if err != nil {
		return fmt.Errorf("failed to update stage comment: %w", err)
	} // TODO check if update was done?

	return nil
}

func (registry *CloudTaskRegistry) UpdateStageStartTime(stage *Stage, tStartUTC time.Time) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(StagesTable),
		Key: map[string]types.AttributeValue{
			"run_uuid": &types.AttributeValueMemberS{Value: stage.TaskRunUUID},
			"n_ord":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", stage.NOrd)},
		},
		UpdateExpression:    aws.String("SET t_start_utc = :t"),
		ConditionExpression: aws.String("attribute_exists(n_ord)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": &types.AttributeValueMemberS{Value: tStartUTC.Format(time.RFC3339)},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}

	_, err := registry.dynamodbClient.UpdateItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to update t_start_utc for stage %v: %w", stage, err)
	}

	return nil
}

func (registry *CloudTaskRegistry) UpdateStageFinishTime(stage *Stage, tFinishUTC time.Time) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(StagesTable),
		Key: map[string]types.AttributeValue{
			"run_uuid": &types.AttributeValueMemberS{Value: stage.TaskRunUUID},
			"n_ord":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", stage.NOrd)},
		},
		UpdateExpression:    aws.String("SET t_finish_utc = :t"),
		ConditionExpression: aws.String("attribute_exists(n_ord)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": &types.AttributeValueMemberS{Value: tFinishUTC.Format(time.RFC3339)},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}

	_, err := registry.dynamodbClient.UpdateItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to update t_finish_utc for stage %v: %w", stage, err)
	}

	return nil
}

func (registry *CloudTaskRegistry) UploadFileForTask(filePath, s3Bucket, taskId, taskRunId string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	s3Path := strings.Join([]string{s3CommonPrefix, taskId, taskRunId, filepath.Base(filePath)}, "/")
	_, err = registry.s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Path),
		Body:   file,
	})
	if err != nil {
		return "", err
	}

	log.Printf("File uploaded to S3: %s", s3Path)
	return s3Path, nil
}

func (registry *CloudTaskRegistry) UploadFileForStage(
	filePath string,
	s3Bucket string,
	taskRun *TaskRun,
	stageName string,
	stageNOrd int,
) (string, error) {
	// By default, we use Standard storage class
	return registry.uploadFileForStage(
		filePath, s3Bucket, taskRun, stageName, stageNOrd, s3types.StorageClassStandard)
}

func (registry *CloudTaskRegistry) UploadExtraFileForStage(
	filePath string,
	s3Bucket string,
	taskRun *TaskRun,
	stageName string,
	stageNOrd int,
) (string, error) {
	// Cold storage is 2x cheaper, so let's use it for extra artifacts that stages may have
	return registry.uploadFileForStage(
		filePath, s3Bucket, taskRun, stageName, stageNOrd, s3types.StorageClassStandardIa)
}

func (registry *CloudTaskRegistry) uploadFileForStage(
	filePath string,
	s3Bucket string,
	taskRun *TaskRun,
	stageName string,
	stageNOrd int,
	storageClass s3types.StorageClass,
) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	stageFolder := fmt.Sprintf("%d_%s", stageNOrd, stageName)
	s3Path := strings.Join(
		[]string{s3CommonPrefix, taskRun.TaskID, taskRun.UUID, stageFolder, filepath.Base(filePath)},
		"/")
	_, err = registry.s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:       aws.String(s3Bucket),
		Key:          aws.String(s3Path),
		Body:         file,
		StorageClass: storageClass,
	})
	if err != nil {
		return "", err
	}

	log.Printf("File uploaded to S3: %s", s3Path)
	return s3Path, nil
}

func (registry *CloudTaskRegistry) DownloadConfigFile(stage *Stage, destination string) error {
	err := registry.DownloadFileFromS3(stage.S3Bucket, stage.Config, destination)
	if err != nil {
		return fmt.Errorf("failed to download config file %q from s3 bucket %q for stage %q of task %s, %w",
			stage.Config, stage.S3Bucket, stage.Name, stage.TaskRunUUID, err)
	}
	return nil
}

func (registry *CloudTaskRegistry) DownloadInputFile(stage *Stage, destination string) error {
	err := registry.DownloadFileFromS3(stage.S3Bucket, stage.Input, destination)
	if err != nil {
		return fmt.Errorf("failed to download input file %q from s3 bucket %q for stage %q of task %s, %w",
			stage.Input, stage.S3Bucket, stage.Name, stage.TaskRunUUID, err)
	}
	return nil
}

func (registry *CloudTaskRegistry) DownloadFileFromS3(s3Bucket, s3Path, destination string) error {
	object, err := registry.s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Path),
	})
	if err != nil {
		return fmt.Errorf("couldn't download file %q from S3 bucket %q, %w", s3Path, s3Bucket, err)
	}
	defer object.Body.Close()

	destFile, err := os.Create(destination)
	if err != nil {
		return fmt.Errorf("failed to create/overwrite destination file %q, %w", destination, err)
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, object.Body)
	if err != nil {
		return fmt.Errorf("failed to copy file content from S3 to local file, %w", err)
	}

	absPath, _ := filepath.Abs(destination)
	log.Printf("Successfully downloaded %q from S3 bucket %q to %q", s3Path, s3Bucket, absPath)
	return nil
}
