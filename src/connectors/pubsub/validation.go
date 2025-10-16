package main

import (
	"fmt"
	"regexp"
)

// validateProjectID validates Google Cloud project ID format
func validateProjectID(id string) error {
	// GCP project IDs must be 6-30 characters, start with lowercase letter,
	// contain only lowercase letters, digits, and hyphens, and end with letter or digit
	matched, err := regexp.MatchString(`^[a-z][-a-z0-9]{4,28}[a-z0-9]$`, id)
	if err != nil {
		return fmt.Errorf("error validating project ID: %w", err)
	}
	if !matched {
		return fmt.Errorf("invalid project ID format: %s (must be 6-30 chars, lowercase letters, digits, hyphens only)", id)
	}
	return nil
}

// validateSubscriptionName validates subscription name format
func validateSubscriptionName(name string) error {
	// Subscription names must start with letter, contain only letters, digits, hyphens, underscores, periods
	matched, err := regexp.MatchString(`^[a-zA-Z][a-zA-Z0-9._-]*$`, name)
	if err != nil {
		return fmt.Errorf("error validating subscription name: %w", err)
	}
	if !matched {
		return fmt.Errorf("invalid subscription name format: %s", name)
	}
	if len(name) > 255 {
		return fmt.Errorf("subscription name too long: %d (max 255)", len(name))
	}
	return nil
}

// validateTopicName validates topic name format
func validateTopicName(name string) error {
	// Topic names must start with letter, contain only letters, digits, hyphens, underscores, periods
	matched, err := regexp.MatchString(`^[a-zA-Z][a-zA-Z0-9._-]*$`, name)
	if err != nil {
		return fmt.Errorf("error validating topic name: %w", err)
	}
	if !matched {
		return fmt.Errorf("invalid topic name format: %s", name)
	}
	if len(name) > 255 {
		return fmt.Errorf("topic name too long: %d (max 255)", len(name))
	}
	return nil
}
