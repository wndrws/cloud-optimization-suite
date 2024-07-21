package main

import (
	"errors"
	"fmt"
	"github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
	"net/http"
)

func downloadConfigFileIfSpecified(stage *cloud_task_registry.Stage, configPath string) *AppError {
	if stage.Config != "" {
		if configPath == "" {
			msg := "config file path is not specified in cloud-connector args"
			return &AppError{errors.New(msg), msg, http.StatusInternalServerError, stage}
		}
		if err := taskRegistry.DownloadConfigFile(stage, configPath); err != nil {
			msg := fmt.Sprintf("couldn't download config file %q from S3 bucket %q", configPath, stage.S3Bucket)
			return &AppError{err, msg, http.StatusInternalServerError, stage}
		}
	}
	return nil
}

func downloadInputFileIfSpecified(stage *cloud_task_registry.Stage, inputFilePath string) *AppError {
	if stage.Input != "" {
		if inputFilePath == "" {
			msg := "input file path is not specified in cloud-connector args"
			return &AppError{errors.New(msg), msg, http.StatusInternalServerError, stage}
		}
		if err := taskRegistry.DownloadInputFile(stage, inputFilePath); err != nil {
			msg := fmt.Sprintf("couldn't download input file %q from S3 bucket %q", inputFilePath, stage.S3Bucket)
			return &AppError{err, msg, http.StatusInternalServerError, stage}
		}
	}
	return nil
}
