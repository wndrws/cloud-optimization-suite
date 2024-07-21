package cloud_task_registry

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"log"
	"strings"
	"time"
)

const finishedTasksQ = "finished-tasks"

func (registry *CloudTaskRegistry) FinishTaskRun(taskRunUUID string) error {
	err := sendMessageToSQS(finishedTasksQ, taskRunUUID, registry.sqsClient)
	if err != nil {
		return fmt.Errorf("error sending message to SQS queue %q, %w", finishedTasksQ, err)
	}
	log.Println("Task run", taskRunUUID, "is marked as finished")
	return nil
}

func (registry *CloudTaskRegistry) PassTaskToStage(stage *Stage) error {
	err := sendMessageToSQS(stage.Name, stage.TaskRunUUID, registry.sqsClient)
	if err != nil {
		return fmt.Errorf("error sending message to SQS queue %q, %w", stage.Name, err)
	}
	log.Println("Passed task to stage", stage.Name)
	return nil
}

func sendMessageToSQS(queueName, messageBody string, svc *sqs.Client) error {
	queueUrl, err := getQueueUrl(queueName, svc)
	if err != nil {
		return fmt.Errorf("error getting SQS queue URL for name %q, %w", queueName, err)
	}
	_, err = svc.SendMessage(context.TODO(), &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueUrl),
		MessageBody: aws.String(messageBody),
	})
	return err
}

func getQueueUrl(queueName string, svc *sqs.Client) (string, error) {
	result, err := svc.GetQueueUrl(context.TODO(), &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", err
	}
	return *result.QueueUrl, nil
}

func (registry *CloudTaskRegistry) WaitForPipelineFinish(taskId, expectedTaskRunUUID string) (string, error) {
	queueURL, err := getQueueUrl(finishedTasksQ, registry.sqsClient)
	if err != nil {
		return "", fmt.Errorf("error getting SQS queue URL for name %q, %w", finishedTasksQ, err)
	}
	log.Println("Waiting for the pipeline to finish...")
	for {
		// Receive messages with long polling
		output, err := registry.sqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20, // Long polling timeout (maximum 20 seconds)
		})
		if err != nil {
			return "", fmt.Errorf("failed to receive messages, %w", err)
		}

		if len(output.Messages) == 0 {
			registry.printStatusReport(taskId, expectedTaskRunUUID)
			// Sleep for a short duration before polling again
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Received a message from %s queue\n", finishedTasksQ)
		if len(output.Messages) > 1 {
			log.Println("Received message count is more than 1! Only the first will be taken.")
		}

		finishedTaskRunUUID := output.Messages[0].Body
		if *finishedTaskRunUUID != expectedTaskRunUUID {
			log.Printf("Pipeline returned %s as finished task but expected %s, keep waiting...\n",
				*finishedTaskRunUUID, expectedTaskRunUUID)
			registry.printStatusReport(taskId, expectedTaskRunUUID)
			time.Sleep(20 * time.Second)
			continue
		} else {
			log.Println("TaskRun", *finishedTaskRunUUID, "finished!")
			_, err = registry.sqsClient.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: output.Messages[0].ReceiptHandle,
			})
			if err != nil {
				fmt.Printf("failed to remove message from the queue (non-critical error), %w", err)
			}
		}

		return *finishedTaskRunUUID, nil
	}
}

func (registry *CloudTaskRegistry) printStatusReport(taskId string, expectedTaskRunUUID string) {
	stagesStatusReport, err := registry.getStagesStatusReport(expectedTaskRunUUID)
	if err != nil {
		log.Printf("failed to get stages status report (non-critical error), %v", err)
	}
	log.Printf("%s (run %s): %s\n", taskId, expectedTaskRunUUID, stagesStatusReport)
}

func (registry *CloudTaskRegistry) getStagesStatusReport(taskRunUUID string) (string, error) {
	stages, err := registry.GetAllStages(taskRunUUID)
	if err != nil {
		return "", fmt.Errorf("failed getting stages information from DB: %w", err)
	}
	stagesStatuses := make([]string, len(stages))
	for i, stage := range stages {
		stagesStatuses[i] = fmt.Sprintf("%s - %s", stage.Name, stage.Status)
	}
	return strings.Join(stagesStatuses, ", "), nil
}
