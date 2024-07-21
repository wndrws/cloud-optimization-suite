#!/bin/bash

export CGO_ENABLED=0

go build -o bin/cloud-connector -a -ldflags '-extldflags "-static"' ./cloud-connector
go build -o bin/cloud-task-runner -a -ldflags '-extldflags "-static"' ./cloud-task-runner
