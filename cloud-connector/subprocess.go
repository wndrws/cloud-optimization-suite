package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"
)

func startCommandAndWait(
	commandFilePath string,
	stage *cloud_task_registry.Stage,
	envVars map[string]string,
) *AppError {
	command, errReadCmdFile := os.ReadFile(commandFilePath) // TODO support direct command instead of a file?
	if errReadCmdFile != nil {
		msg := "unable to read command file"
		return &AppError{errReadCmdFile, msg, http.StatusInternalServerError, stage}
	}

	// Create environment variables from the Parameters map
	// env := []string{}
	for key, value := range envVars { // TODO Does this work?
		//env = append(env, fmt.Sprintf("%s=%s", key, value))
		if err := os.Setenv(key, value); err != nil {
			msg := fmt.Sprintf("couldn't set env var %s=%s", key, value)
			return &AppError{err, msg, http.StatusInternalServerError, stage}
		}
	}

	cmd := exec.CommandContext(context.Background(), "/bin/sh", "-c", string(command))
	cmd.Env = nil // append(cmd.Env, env...) // TODO Does this work?
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		msg := fmt.Sprintf("unable to start shell subprocess %q", string(command))
		return &AppError{err, msg, http.StatusInternalServerError, stage}
	}

	go monitorSubprocess(cmd)

	if err := cmd.Wait(); err != nil || cmd.ProcessState.ExitCode() != 0 {
		if err == nil {
			err = errors.New("non-zero exit code from subprocess")
		}
		msg := fmt.Sprintf("subprocess failed with error, exit-code %d", cmd.ProcessState.ExitCode())
		return &AppError{err, msg, http.StatusInternalServerError, stage}
	}
	return nil
}

func monitorSubprocess(cmd *exec.Cmd) {
	for {
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			log.Println("Subprocess has exited")
			return
		}
		log.Println("Subprocess is still running")
		time.Sleep(5 * time.Second)
	}
}
