package cloud_task_registry

import "time"

type TaskRun struct {
	TaskID         string            `dynamodbav:"task_id"`  // PK
	UUID           string            `dynamodbav:"run_uuid"` // SK, SGI PK
	Parameters     map[string]string `dynamodbav:"parameters"`
	Results        map[string]string `dynamodbav:"results,omitempty"`
	TaskDefinition string            `dynamodbav:"task_definition"`
	CreationTime   *time.Time        `dynamodbav:"creation_time,omitempty"`
}

type Stage struct {
	TaskRunUUID string     `dynamodbav:"run_uuid"` // PK
	NOrd        int        `dynamodbav:"n_ord"`    // SK
	Name        string     `dynamodbav:"name"`     // SGI SK
	Status      string     `dynamodbav:"status"`
	Config      string     `dynamodbav:"config,omitempty"`
	Input       string     `dynamodbav:"input,omitempty"`
	Output      string     `dynamodbav:"output,omitempty"`
	TStartUTC   *time.Time `dynamodbav:"t_start_utc,omitempty"`
	TFinishUTC  *time.Time `dynamodbav:"t_finish_utc,omitempty"`
	Executor    string     `dynamodbav:"executor,omitempty"`
	S3Bucket    string     `dynamodbav:"s3_bucket"`
	Comments    string     `dynamodbav:"comments,omitempty"`
	Next        []string   `dynamodbav:"next,omitempty"` // name(s) of stage(s) to execute next
}

// очереди в SQS создавать по именам stage'ей, которым предназначены сообщения в очередях

const (
	StageStatus_Pending    = "Pending"
	StageStatus_InProgress = "InProgress"
	StageStatus_Success    = "Success"
	StageStatus_Error      = "Error"
	StageStatus_Cancelled  = "Cancelled"
)

const StageInitialStatus = StageStatus_Pending

const TasksTable = "task_runs"
const StagesTable = "task_stages"
