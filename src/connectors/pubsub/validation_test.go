package main

import (
	"testing"
)

func TestValidateProjectID(t *testing.T) {
	tests := []struct {
		name      string
		projectID string
		wantErr   bool
	}{
		{
			name:      "valid project ID",
			projectID: "my-project-123",
			wantErr:   false,
		},
		{
			name:      "valid project ID with min length",
			projectID: "abc123",
			wantErr:   false,
		},
		{
			name:      "valid project ID with max length",
			projectID: "a" + "-test-project-long-name-ok" + "9",
			wantErr:   false,
		},
		{
			name:      "invalid - too short",
			projectID: "abc",
			wantErr:   true,
		},
		{
			name:      "invalid - starts with number",
			projectID: "1project",
			wantErr:   true,
		},
		{
			name:      "invalid - contains uppercase",
			projectID: "myProject",
			wantErr:   true,
		},
		{
			name:      "invalid - contains underscore",
			projectID: "my_project",
			wantErr:   true,
		},
		{
			name:      "invalid - starts with hyphen",
			projectID: "-project",
			wantErr:   true,
		},
		{
			name:      "invalid - ends with hyphen",
			projectID: "project-",
			wantErr:   true,
		},
		{
			name:      "invalid - empty",
			projectID: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateProjectID(tt.projectID)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateProjectID() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTopicName(t *testing.T) {
	tests := []struct {
		name      string
		topicName string
		wantErr   bool
	}{
		{
			name:      "valid topic name",
			topicName: "my-topic",
			wantErr:   false,
		},
		{
			name:      "valid with underscore",
			topicName: "my_topic",
			wantErr:   false,
		},
		{
			name:      "valid with period",
			topicName: "my.topic",
			wantErr:   false,
		},
		{
			name:      "valid with uppercase",
			topicName: "MyTopic",
			wantErr:   false,
		},
		{
			name:      "invalid - starts with number",
			topicName: "1topic",
			wantErr:   true,
		},
		{
			name:      "invalid - starts with hyphen",
			topicName: "-topic",
			wantErr:   true,
		},
		{
			name:      "invalid - too long",
			topicName: string(make([]byte, 256)),
			wantErr:   true,
		},
		{
			name:      "invalid - empty",
			topicName: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTopicName(tt.topicName)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTopicName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateSubscriptionName(t *testing.T) {
	tests := []struct {
		name             string
		subscriptionName string
		wantErr          bool
	}{
		{
			name:             "valid subscription name",
			subscriptionName: "my-subscription",
			wantErr:          false,
		},
		{
			name:             "valid with underscore",
			subscriptionName: "my_subscription",
			wantErr:          false,
		},
		{
			name:             "valid with period",
			subscriptionName: "my.subscription",
			wantErr:          false,
		},
		{
			name:             "valid with uppercase",
			subscriptionName: "MySubscription",
			wantErr:          false,
		},
		{
			name:             "invalid - starts with number",
			subscriptionName: "1subscription",
			wantErr:          true,
		},
		{
			name:             "invalid - starts with hyphen",
			subscriptionName: "-subscription",
			wantErr:          true,
		},
		{
			name:             "invalid - too long",
			subscriptionName: string(make([]byte, 256)),
			wantErr:          true,
		},
		{
			name:             "invalid - empty",
			subscriptionName: "",
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSubscriptionName(tt.subscriptionName)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSubscriptionName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewSourceConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *SourceConfig
		wantErr bool
	}{
		{
			name: "valid configuration",
			cfg: &SourceConfig{
				ProjectID:    "my-project-123",
				Subscription: "mySubscription",
				Topic:        "myTopic",
			},
			wantErr: false,
		},
		{
			name: "invalid project ID",
			cfg: &SourceConfig{
				ProjectID:    "INVALID_PROJECT",
				Subscription: "mySubscription",
				Topic:        "myTopic",
			},
			wantErr: true,
		},
		{
			name: "invalid subscription name",
			cfg: &SourceConfig{
				ProjectID:    "my-project-123",
				Subscription: "123invalid",
				Topic:        "myTopic",
			},
			wantErr: true,
		},
		{
			name: "invalid topic name",
			cfg: &SourceConfig{
				ProjectID:    "my-project-123",
				Subscription: "mySubscription",
				Topic:        "123invalid",
			},
			wantErr: true,
		},
		{
			name: "maxMessages too large",
			cfg: &SourceConfig{
				ProjectID:    "my-project-123",
				Subscription: "mySubscription",
				Topic:        "myTopic",
				MaxMessages:  20000,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSource(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewSource() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewTargetConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *TargetConfig
		wantErr bool
	}{
		{
			name: "invalid project ID",
			cfg: &TargetConfig{
				ProjectID: "INVALID_PROJECT",
				Topic:     "myTopic",
			},
			wantErr: true,
		},
		{
			name: "invalid topic name",
			cfg: &TargetConfig{
				ProjectID: "my-project-123",
				Topic:     "123invalid",
			},
			wantErr: true,
		},
		{
			name: "maxMessageSize too large",
			cfg: &TargetConfig{
				ProjectID:      "my-project-123",
				Topic:          "myTopic",
				MaxMessageSize: 20000000,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTarget(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewTarget() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
