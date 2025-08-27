package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/wndrws/cloud-optimization-suite/cloud-task-registry"
)

func startCommandAndWait(
	command string,
	stage *cloud_task_registry.Stage,
	envVars map[string]string,
) *AppError {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create environment variables from the Parameters map
	// env := []string{}
	for key, value := range envVars { // TODO Does this work?
		//env = append(env, fmt.Sprintf("%s=%s", key, value))
		if err := os.Setenv(key, value); err != nil {
			msg := fmt.Sprintf("couldn't set env var %s=%s", key, value)
			return &AppError{err, msg, http.StatusInternalServerError, stage}
		}
	}

	cmd := exec.CommandContext(context.Background(), "/bin/sh", "-c", command)
	cmd.Env = nil // append(cmd.Env, env...) // TODO Does this work?
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	go launchTaskCancellationListener(ctx, cmd, stage)
	if err := cmd.Start(); err != nil {
		msg := fmt.Sprintf("unable to start shell subprocess %q", command)
		return &AppError{err, msg, http.StatusInternalServerError, stage}
	}
	log.Println("Started subprocess", command)

	go monitorSubprocess(cmd)

	if err := cmd.Wait(); err != nil || cmd.ProcessState.ExitCode() != 0 {
		if taskWasCancelled, _ := taskRegistry.IsCancelled(stage.TaskRunUUID); taskWasCancelled {
			log.Println("Subprocess was interrupted and finished with exit-code", cmd.ProcessState.ExitCode())
		} else {
			if err == nil {
				err = errors.New("non-zero exit code from subprocess")
			}
			msg := fmt.Sprintf("subprocess failed with error, exit-code %d", cmd.ProcessState.ExitCode())
			return &AppError{err, msg, http.StatusInternalServerError, stage}
		}
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

func launchTaskCancellationListener(ctx context.Context, cmd *exec.Cmd, stage *cloud_task_registry.Stage) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			taskCancelled, err := taskRegistry.IsCancelled(stage.TaskRunUUID)
			if err != nil {
				log.Println("Couldn't check the task run cancellation (will retry in 5s):", err)
			}
			if taskCancelled {
				err = cmd.Process.Signal(syscall.SIGTERM)
				if err != nil {
					log.Println("Couldn't send SIGTERM to the job (will retry in 5s):", err)
				}
			}
			time.Sleep(5 * time.Second)
		}
	}
}
