package cloud_task_registry

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
)

func checkTableExists(d *dynamodb.Client, name string) bool {
	tables, err := d.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		log.Fatal("ListTables failed", err)
	}
	for _, n := range tables.TableNames {
		if n == name {
			return true
		}
	}
	return false
}

func createTasksTable(svc *dynamodb.Client) error {
	tableExists := checkTableExists(svc, TasksTable)

	if tableExists {
		log.Println(TasksTable, "table already exists")
		return nil
	}

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(TasksTable),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("task_id"),
				KeyType:       types.KeyTypeHash, // Partition key
			},
			{
				AttributeName: aws.String("run_uuid"),
				KeyType:       types.KeyTypeRange, // Sort key
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("TaskRunUUIDIndex"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("run_uuid"),
						KeyType:       types.KeyTypeHash, // Partition key
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("task_id"),
				AttributeType: types.ScalarAttributeTypeS, // UUID as string
			},
			{
				AttributeName: aws.String("run_uuid"),
				AttributeType: types.ScalarAttributeTypeS, // UUID as string
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	}

	_, err := svc.CreateTable(context.TODO(), input)
	if err == nil {
		log.Println(TasksTable, "table created")
	}
	return err
}

func createStagesTable(svc *dynamodb.Client) error {
	tableExists := checkTableExists(svc, StagesTable)

	if tableExists {
		log.Println(StagesTable, "table already exists")
		return nil
	}

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(StagesTable),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("run_uuid"),
				KeyType:       types.KeyTypeHash, // Partition key
			},
			{
				AttributeName: aws.String("n_ord"),
				KeyType:       types.KeyTypeRange, // Sort key
			},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("StageNameIndex"),
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("run_uuid"),
						KeyType:       types.KeyTypeHash, // Partition key
					},
					{
						AttributeName: aws.String("name"),
						KeyType:       types.KeyTypeRange, // Sort key
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("run_uuid"),
				AttributeType: types.ScalarAttributeTypeS, // UUID as string
			},
			{
				AttributeName: aws.String("n_ord"),
				AttributeType: types.ScalarAttributeTypeN, // n_ord as number
			},
			{
				AttributeName: aws.String("name"),
				AttributeType: types.ScalarAttributeTypeS, // name as string
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		//ProvisionedThroughput: &types.ProvisionedThroughput{
		//	ReadCapacityUnits:  aws.Int64(5),
		//	WriteCapacityUnits: aws.Int64(5),
		//},
	}

	_, err := svc.CreateTable(context.TODO(), input)
	if err == nil {
		log.Println(StagesTable, "table created")
	}
	return err
}

func migrate(svc *dynamodb.Client) error {
	if err := createTasksTable(svc); err != nil {
		errTasks := fmt.Errorf("failed to create %q table: %w", TasksTable, err)
		return errTasks
	}
	if err := createStagesTable(svc); err != nil {
		errStages := fmt.Errorf("failed to create %q table: %w", StagesTable, err)
		return errStages
	}
	return nil
}
