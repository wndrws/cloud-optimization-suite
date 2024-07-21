package cloud_task_registry

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const s3CommonPrefix = "task-registry"

type CloudTaskRegistry struct {
	dynamodbClient *dynamodb.Client
	s3Client       *s3.Client
	sqsClient      *sqs.Client
}

func New(dynamoDocApiEndpoint string) (*CloudTaskRegistry, error) {
	configForDynamoDB, err := getAwsConfigForDynamoDB(dynamoDocApiEndpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config for DynamoDB, %w", err)
	}

	configForS3, err := getAwsConfigForS3()
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config for S3, %w", err)
	}

	configForSQS, err := getAwsConfigForSQS()
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config for S3, %w", err)
	}

	svc := dynamodb.NewFromConfig(configForDynamoDB)
	if err = migrate(svc); err != nil {
		return nil, err
	}

	return &CloudTaskRegistry{
		dynamodbClient: svc,
		s3Client:       s3.NewFromConfig(configForS3),
		sqsClient:      sqs.NewFromConfig(configForSQS),
	}, nil
}

func getAwsConfigForDynamoDB(dynamoDocApiEndpoint string) (aws.Config, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: dynamoDocApiEndpoint, SigningRegion: "ru-central1"}, nil
		},
	)

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	return cfg, err
}

func getAwsConfigForS3() (aws.Config, error) {
	// See https://yandex.cloud/ru/docs/storage/tools/aws-sdk-go
	customResolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID && region == "ru-central1" {
				return aws.Endpoint{
					PartitionID:   "yc",
					URL:           "https://storage.yandexcloud.net",
					SigningRegion: "ru-central1",
				}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
		})

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	return cfg, err
}

func getAwsConfigForSQS() (aws.Config, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           "https://message-queue.api.cloud.yandex.net",
				SigningRegion: "ru-central1",
			}, nil
		})

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	return cfg, err
}
