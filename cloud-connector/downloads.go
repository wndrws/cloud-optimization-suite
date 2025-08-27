package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	cloud_task_registry "github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
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
		if strings.HasSuffix(inputFilePath, "/") {
			if err := downloadInputToFolder(stage, inputFilePath); err != nil {
				msg := fmt.Sprintf("couldn't download input artifacts from S3 bucket %q and place them into folder %q",
					inputFilePath, stage.S3Bucket)
				return &AppError{err, msg, http.StatusInternalServerError, stage}
			}
		} else {
			if err := taskRegistry.DownloadInputFile(stage, inputFilePath); err != nil {
				msg := fmt.Sprintf("couldn't download input file %q from S3 bucket %q", inputFilePath, stage.S3Bucket)
				return &AppError{err, msg, http.StatusInternalServerError, stage}
			}
		}
	}
	return nil
}

func downloadInputToFolder(stage *cloud_task_registry.Stage, inputFilePath string) error {
	tempfile, err := os.CreateTemp("", stage.Name+"-input")
	if err != nil {
		return fmt.Errorf("couldn't create temp file for downloading the input artifact(s)")
	}
	defer func() {
		if err := os.Remove(tempfile.Name()); err != nil {
			fmt.Println("Couldn't remove the temporary file", tempfile, err)
		} else {
			fmt.Println("Cleaned up the temporary file:", tempfile.Name())
		}
	}()

	if err := taskRegistry.DownloadInputFile(stage, tempfile.Name()); err != nil {
		return fmt.Errorf("couldn't download input file %q from S3 bucket %q to temporary file %q",
			inputFilePath, stage.S3Bucket, tempfile.Name())
	}

	isArchive, err := Is7z(tempfile.Name())
	if err != nil {
		return fmt.Errorf("couldn't check if the file is a 7z-archive %q", tempfile.Name())
	}

	if isArchive {
		if err := Extract7z(tempfile.Name(), inputFilePath); err != nil {
			return fmt.Errorf("couldn't extract 7z archive %q to folder %q", tempfile.Name(), inputFilePath)
		}
	} else {
		if err := moveFile(tempfile.Name(), inputFilePath); err != nil {
			return fmt.Errorf("couldn't move file %q to folder %q", tempfile.Name(), inputFilePath)
		}
	}
	return nil
}

func moveFile(sourcePath, destFolder string) error {
	inputFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("couldn't open source file: %v", err)
	}
	defer inputFile.Close()

	outputFile, err := os.Create(path.Join(destFolder, path.Base(sourcePath)))
	if err != nil {
		return fmt.Errorf("couldn't open dest file: %v", err)
	}
	defer outputFile.Close()

	_, err = io.Copy(outputFile, inputFile)
	if err != nil {
		return fmt.Errorf("couldn't copy to dest from source: %v", err)
	}

	inputFile.Close()
	err = os.Remove(sourcePath)
	if err != nil {
		return fmt.Errorf("couldn't remove source file: %v", err)
	}
	return nil
}
