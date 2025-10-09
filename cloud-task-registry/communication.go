package cloud_task_registry

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const finishedTasksQ = "finished-tasks"

const longPollingInterval = 20 // seconds

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

func (registry *CloudTaskRegistry) WaitForPipelineFinish(
	taskId string,
	expectedTaskRunUUID string,
	wasCancelled chan bool,
) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-wasCancelled
		cancel()
	}()
	defer func() {
		wasCancelled <- false
	}()

	queueURL, err := getQueueUrl(finishedTasksQ, registry.sqsClient)
	if err != nil {
		return "", fmt.Errorf("error getting SQS queue URL for name %q, %w", finishedTasksQ, err)
	}
	log.Println("Waiting for the pipeline to finish...")
	for {
		// Receive messages with long polling
		output, err := registry.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     longPollingInterval,
		})
		if err != nil {
			return "", fmt.Errorf("failed to receive messages, %w", err)
		}

		if len(output.Messages) == 0 {
			registry.printStatusReport(taskId, expectedTaskRunUUID)
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
			err := makeMessageMaximallyVisible(finishedTasksQ, *output.Messages[0].ReceiptHandle, registry.sqsClient)
			if err != nil {
				log.Printf("Failed to set message visibility timeout to 0 due to an error: %v\n", err)
				log.Println("You can try sending SIGSTOP and SIGCONT to one of the task runners " +
					"to break the tie between them if this is the case.")
			}
			registry.printStatusReport(taskId, expectedTaskRunUUID)
			interrupted := SleepInterruptibly(ctx, time.Duration(rand.Intn(3000))*time.Millisecond)
			if interrupted {
				return expectedTaskRunUUID, nil
			} else {
				continue
			}
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

func SleepInterruptibly(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
		return true
	case <-t.C:
	}
	return false
}

func makeMessageMaximallyVisible(queueName, receiptHandle string, svc *sqs.Client) error {
	queueUrl, err := getQueueUrl(queueName, svc)
	if err != nil {
		return fmt.Errorf("error getting SQS queue URL for name %q, %w", queueName, err)
	}

	input := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queueUrl),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: 0,
	}

	_, err = svc.ChangeMessageVisibility(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to change message visibility: %w", err)
	}
	return nil
}

func (registry *CloudTaskRegistry) WaitForDLQ(
	dlqName string,
	expectedTaskRunUUID string,
	wasCancelled chan bool,
) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-wasCancelled
		cancel()
	}()
	defer func() {
		wasCancelled <- false
	}()

	queueURL, err := getQueueUrl(dlqName, registry.sqsClient)
	if err != nil {
		return "", fmt.Errorf("error getting SQS queue URL for name %q, %w", finishedTasksQ, err)
	}
	log.Println("Waiting for the dead-letter queue...")
	for {
		// Receive messages with long polling
		output, err := registry.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     longPollingInterval,
		})
		if err != nil {
			return "", fmt.Errorf("failed to receive messages, %w", err)
		}

		if len(output.Messages) == 0 {
			continue
		}

		log.Printf("Received a message from the dead-letter queue %s\n", dlqName)
		if len(output.Messages) > 1 {
			log.Println("Received message count is more than 1! Only the first will be taken.")
		}

		failedTaskRunUUID := output.Messages[0].Body
		if *failedTaskRunUUID != expectedTaskRunUUID {
			log.Printf("DLQ returned %s as a failed task but expected %s, keep waiting...\n",
				*failedTaskRunUUID, expectedTaskRunUUID)
			err := makeMessageMaximallyVisible(dlqName, *output.Messages[0].ReceiptHandle, registry.sqsClient)
			if err != nil {
				log.Printf("Failed to set message visibility timeout to 0 due to an error: %v\n", err)
				log.Println("You can try sending SIGSTOP and SIGCONT to one of the task runners " +
					"to break the tie between them if this is the case.")
			}
			interrupted := SleepInterruptibly(ctx, time.Duration(rand.Intn(3000))*time.Millisecond)
			if interrupted {
				return expectedTaskRunUUID, nil
			} else {
				continue
			}
		} else {
			log.Println("TaskRun", *failedTaskRunUUID, "failed!")
			_, err = registry.sqsClient.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: output.Messages[0].ReceiptHandle,
			})
			if err != nil {
				fmt.Printf("failed to remove message from the queue (non-critical error), %w", err)
			}
		}

		return *failedTaskRunUUID, nil
	}
}
