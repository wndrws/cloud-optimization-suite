package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"

	cloud_task_registry "github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
)

func uploadOutputFile(
	outputFilePath string,
	task *cloud_task_registry.TaskRun,
	stage *cloud_task_registry.Stage,
) (string, *AppError) {
	if outputFilePath != "" {
		fileToUpload := outputFilePath
		fileInfo, err := os.Stat(outputFilePath)
		if err != nil {
			msg := fmt.Sprintf("unable to stat path %q, %v", outputFilePath, err)
			return "", &AppError{err, msg, http.StatusInternalServerError, stage}
		}
		if fileInfo.IsDir() {
			archivePath := path.Join(os.TempDir(), stage.Name+"-output.7z")
			defer func() {
				if err := os.Remove(archivePath); err != nil {
					fmt.Println("Couldn't remove the temporary file", archivePath, err)
				} else {
					fmt.Println("Cleaned up the archive:", archivePath)
				}
			}()
			log.Printf("output artifact path %s points at a directory, archiving...", outputFilePath)
			archiveCmd := exec.Command("7zz", "a", archivePath, outputFilePath)
			archiveCmd.Stdout = os.Stdout
			archiveCmd.Stderr = os.Stderr
			errRun := archiveCmd.Run()
			if errRun != nil {
				err := fmt.Errorf("failed to compress directory %q, %w", outputFilePath, errRun)
				return "", &AppError{err, err.Error(), http.StatusInternalServerError, stage}
			}
			fileToUpload = archivePath
			log.Println("Successfully created archive:", archivePath)
		}
		s3PathForOutput, err := taskRegistry.UploadFileForStage(
			fileToUpload, stage.S3Bucket, task, stage.Name, stage.NOrd)
		if err != nil {
			msg := fmt.Sprintf("error uploading file %q to S3 bucket %q", fileToUpload, stage.S3Bucket)
			return "", &AppError{err, msg, http.StatusInternalServerError, stage}
		}
		if err := taskRegistry.UpdateStageOutput(stage, s3PathForOutput); err != nil {
			msg := "error setting output for stage"
			return "", &AppError{err, msg, http.StatusInternalServerError, stage}
		}
		return s3PathForOutput, nil
	} else {
		fmt.Println("--output-file-path is not specified, so nothing is uploaded to S3 from this stage")
		return "", nil
	}
}

func uploadExtraArtifactsAndUpdateStageComment(
	extraArtifactsPaths []string,
	task *cloud_task_registry.TaskRun,
	stage *cloud_task_registry.Stage,
) {
	if uploaded, appErr := uploadExtraArtifacts(extraArtifactsPaths, task, stage); appErr != nil {
		comment := fmt.Sprintf("Extra artifacts upload failed! Uploaded %d (%v) out of %d (%v) files, %v",
			len(uploaded), uploaded, len(extraArtifactsPaths), extraArtifactsPaths, appErr.Error)
		log.Println(comment)
		if err := taskRegistry.UpdateStageComment(stage, comment); err != nil {
			msg := fmt.Sprintf("error updating comment for stage %s of task %s", stage.Name, stage.TaskRunUUID)
			log.Println(msg)
		}
	} else {
		comment := fmt.Sprintf("Uploaded %d extra artifacts: %v", len(uploaded), uploaded)
		log.Println(comment)
		if err := taskRegistry.UpdateStageComment(stage, comment); err != nil {
			msg := fmt.Sprintf("error updating comment for stage %s of task %s", stage.Name, stage.TaskRunUUID)
			log.Println(msg)
		}
	}
}

func uploadExtraArtifacts(
	extraArtifactsPaths []string,
	task *cloud_task_registry.TaskRun,
	stage *cloud_task_registry.Stage,
) ([]string, *AppError) {
	uploaded := make([]string, 0, len(extraArtifactsPaths))
	for _, extra := range extraArtifactsPaths {
		fileToUpload := extra
		fileInfo, err := os.Stat(extra)
		if err != nil {
			msg := fmt.Sprintf("unable to stat path %q, %v", extra, err)
			return uploaded, &AppError{err, msg, http.StatusInternalServerError, stage}
		}
		if fileInfo.IsDir() {
			log.Printf("extra artifact path %s points at a directory, archiving...", extra)
			archivePath := extra + ".7z"
			archiveCmd := exec.Command("7zz", "a", path.Base(archivePath), extra)
			archiveCmd.Stdout = os.Stdout
			archiveCmd.Stderr = os.Stderr
			errRun := archiveCmd.Run()
			if errRun != nil {
				err := fmt.Errorf("failed to compress directory %q, %w", extra, errRun)
				return uploaded, &AppError{err, err.Error(), http.StatusInternalServerError, stage}
			}
			fileToUpload = archivePath
			log.Println("Successfully created archive:", archivePath)
			defer func(name string) {
				err := os.Remove(name)
				if err != nil {
					fmt.Println("Couldn't remove the archive:", archivePath, err)
				} else {
					fmt.Println("Cleaned up the archive:", archivePath)
				}
			}(archivePath) // Clean up the ZIP file after upload
		}
		s3Path, err := taskRegistry.UploadExtraFileForStage(
			fileToUpload, stage.S3Bucket, task, stage.Name, stage.NOrd)
		if err != nil {
			msg := fmt.Sprintf("error uploading file %q (extra artifact) to S3 bucket %q", fileToUpload, stage.S3Bucket)
			return uploaded, &AppError{err, msg, http.StatusInternalServerError, stage}
		}
		uploaded = append(uploaded, s3Path)
	}
	return uploaded, nil
}
