package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type RequestBody struct {
	Messages []Message `json:"messages"`
}

type Message struct {
	EventMetadata EventMetadata `json:"event_metadata"`
	Details       Details       `json:"details"`
}

type EventMetadata struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	CreatedAt string `json:"created_at"`
	CloudID   string `json:"cloud_id"`
	FolderID  string `json:"folder_id"`
}

type Details struct {
	QueueID string         `json:"queue_id"`
	Message MessageDetails `json:"message"`
}

type MessageDetails struct {
	MessageID              string               `json:"message_id"`
	MD5OfBody              string               `json:"md5_of_body"`
	Body                   string               `json:"body"`
	Attributes             map[string]string    `json:"attributes"`
	MessageAttributes      map[string]Attribute `json:"message_attributes"`
	MD5OfMessageAttributes string               `json:"md5_of_message_attributes"`
}

type Attribute struct {
	DataType    string `json:"data_type"`
	StringValue string `json:"string_value"`
}

func extractSQSMessageBodyFromYandexCloudTriggerRequest(r *http.Request) (string, *AppError) {
	if r.Method != http.MethodPost {
		return "", &AppError{
			Error:   errors.New("invalid request method"),
			Message: "Only POST is accepted",
			Code:    http.StatusMethodNotAllowed,
		}
	}

	var reqBody RequestBody
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		return "", &AppError{
			Error:   err,
			Message: "Unable to parse request body",
			Code:    http.StatusBadRequest,
		}
	}

	msgCount := len(reqBody.Messages)
	if msgCount != 1 {
		return "", &AppError{
			Error:   fmt.Errorf("request body has %d messages, but only 1 is allowed", msgCount),
			Message: "Request body must contain exactly one message",
			Code:    http.StatusBadRequest,
		}
	}

	messageBody := reqBody.Messages[0].Details.Message.Body
	return messageBody, nil
}
