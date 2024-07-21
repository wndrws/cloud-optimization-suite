package main

import (
	"fmt"
	"github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
	"time"
)

func printTaskReportWithAllStages(task *cloud_task_registry.TaskRun, stages []cloud_task_registry.Stage) {
	fmt.Printf("Finished task run:\n")
	fmt.Printf("  UUID: %s\n", task.UUID)
	fmt.Printf("  Parameters: %v\n", task.Parameters)
	fmt.Printf("  Results: %v\n", task.Results)
	fmt.Printf("  Task Definition: %s\n\n", "[not shown here]")

	switch {
	case allStagesHaveStatus(stages, cloud_task_registry.StageStatus_Success):
		fmt.Printf("All stages finished successfully!\n")
	case anyStageHasStatus(stages, cloud_task_registry.StageStatus_Error):
		fmt.Printf("Error on some stage(s)!\n")
	case anyStageHasStatus(stages, cloud_task_registry.StageStatus_InProgress):
		fmt.Printf("Some stage has status InProgress! This is probably an error!\n")
	case anyStageHasStatus(stages, cloud_task_registry.StageStatus_Pending):
		fmt.Printf("Some stage has status Pending! This is probably an error!\n")
	case anyStageHasStatus(stages, cloud_task_registry.StageStatus_Cancelled):
		fmt.Printf("The task was cancelled!\n")
	}

	fmt.Printf("Stages:\n")
	fmt.Println()
	for _, stage := range stages {
		fmt.Printf("  - Name: %s\n", stage.Name)
		fmt.Printf("    NOrd: %d\n", stage.NOrd)
		fmt.Printf("    Status: %s\n", stage.Status)
		fmt.Printf("    Config: %s\n", stage.Config)
		fmt.Printf("    Input: %s\n", stage.Input)
		fmt.Printf("    Output: %s\n", stage.Output)
		fmt.Printf("    S3Bucket: %s\n", stage.S3Bucket)
		fmt.Printf("    Next: %s\n", stage.Next)
		if stage.TStartUTC != nil {
			fmt.Printf("    Start Time: %s\n", stage.TStartUTC.Format(time.DateTime))
		}
		if stage.TFinishUTC != nil {
			fmt.Printf("    Finish Time: %s\n", stage.TFinishUTC.Format(time.DateTime))
		}
		if stage.Executor != "" {
			fmt.Printf("    Executor: %s\n", stage.Executor)
		}
		if stage.Comments != "" {
			fmt.Printf("    Comments: %s\n", stage.Comments)
		}
		fmt.Println()
	}

	printStagesTimeSummary(task, stages)
}

// Function to calculate time spent on each stage and print a summary
func printStagesTimeSummary(task *cloud_task_registry.TaskRun, stages []cloud_task_registry.Stage) {
	fmt.Printf("Stages processing times:\n")
	durations := make([]time.Duration, len(stages))
	var firstStageStart, lastStageFinish *time.Time
	for i, stage := range stages {
		if i == 0 && stage.TStartUTC != nil {
			firstStageStart = stage.TStartUTC
		}
		if stage.TStartUTC != nil && stage.TFinishUTC != nil {
			lastStageFinish = stage.TFinishUTC
			duration := stage.TFinishUTC.Sub(*stage.TStartUTC)
			durations[i] = duration
			fmt.Printf("    %s: %s\n", stage.Name, duration)
		} else {
			fmt.Printf("    %s: %s\n", stage.Name, "N/A")
		}
	}

	totalDuration := time.Duration(0)
	for _, duration := range durations {
		totalDuration += duration
	}

	fmt.Printf("Total processing time: %s\n", totalDuration)
	if firstStageStart != nil && lastStageFinish != nil {
		wallClockForAllStages := lastStageFinish.Sub(*firstStageStart)
		fmt.Printf("Wall clock time since first stage start: %s\n", wallClockForAllStages)
	} else {
		fmt.Println(" Wall clock time since first stage start: N/A")
	}
	if task.CreationTime != nil {
		if lastStageFinish != nil {
			wallClockForTask := lastStageFinish.Sub(*task.CreationTime)
			fmt.Printf("Wall clock time since task creation: %s\n", wallClockForTask)
		} else {
			wallClockForTaskFallBack := time.Now().UTC().Sub(*task.CreationTime)
			fmt.Printf("Wall clock time since task creation (fallback using time.Now().UTC()): %s\n", wallClockForTaskFallBack)
		}
	} else {
		fmt.Println("Wall clock time since task creation: N/A")
	}
}

func allStagesHaveStatus(stages []cloud_task_registry.Stage, status string) bool {
	for _, stage := range stages {
		if stage.Status != status {
			return false
		}
	}
	return true
}

func anyStageHasStatus(stages []cloud_task_registry.Stage, status string) bool {
	for _, stage := range stages {
		if stage.Status == status {
			return true
		}
	}
	return false
}
