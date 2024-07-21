package main

import (
	"bufio"
	"fmt"
	"github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
)

type StageYAML struct {
	Name     string   `yaml:"name"`
	Config   string   `yaml:"config"`
	Executor string   `yaml:"executor"`
	Next     []string `yaml:"next"`
}

func createStages(
	registry *cloud_task_registry.CloudTaskRegistry,
	taskRun *cloud_task_registry.TaskRun,
	stagesYamlPath string,
	s3Bucket string,
) ([]cloud_task_registry.Stage, error) {
	data, err := os.ReadFile(stagesYamlPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var stagesYAML []StageYAML
	if err = yaml.Unmarshal(data, &stagesYAML); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	stages := make([]cloud_task_registry.Stage, len(stagesYAML))
	notFoundNextStages := make(map[string]string)
	for i, stageYAML := range stagesYAML {
		stageNOrd := i + 1
		var s3Path = ""
		if stageYAML.Config != "" {
			_, err = os.Stat(stageYAML.Config)
			if err != nil {
				return nil, fmt.Errorf("cannot stat stage %v config file: %v", stageYAML, err)
			}
			s3Path, err = registry.UploadFileForStage(stageYAML.Config, s3Bucket, taskRun, stageYAML.Name, stageNOrd)
			if err != nil {
				return nil, fmt.Errorf("error uploading stage config file to S3, %v", err)
			}
		}
		stages[i] = cloud_task_registry.Stage{
			TaskRunUUID: taskRun.UUID,
			NOrd:        stageNOrd,
			Name:        stageYAML.Name,
			Status:      cloud_task_registry.StageInitialStatus,
			Config:      s3Path,
			Executor:    stageYAML.Executor,
			S3Bucket:    s3Bucket,
			Next:        stageYAML.Next,
		}
		for _, nextStage := range stageYAML.Next {
			notFoundNextStages[nextStage] = nextStage
		}
		delete(notFoundNextStages, stageYAML.Name)
	}

	if len(notFoundNextStages) > 0 {
		return nil, fmt.Errorf("some stage(s) reference next stage(s) that were not found: %v", notFoundNextStages)
	}

	return stages, nil
}

func readKeyValueFile(filePath string) (map[string]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	kvMap := make(map[string]string)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		// Skip empty lines and lines starting with #
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid line: %s (btw, comments are supported only on separate lines)", line)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		kvMap[key] = value
	}

	if err = scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return kvMap, nil
}
