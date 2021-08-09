package azure

import (
	"testing"

	"github.com/batchcorp/plumber/options"
)

func TestValidateReadOptions_Passes(t *testing.T) {
	opts := &options.Options{Azure: &options.AzureServiceBusOptions{
		Queue: "test-queue",
		Topic: "",
	}}

	if err := validateReadOptions(opts); err != nil {
		t.Errorf("validateReadOptions() returned: %s, expected: nil", err)
	}
}

func TestValidateReadOptions_TopicOrQueue(t *testing.T) {
	opts := &options.Options{Azure: &options.AzureServiceBusOptions{
		Queue: "test-queue",
		Topic: "test-topic",
	}}

	if err := validateReadOptions(opts); err == nil {
		t.Errorf("validateReadOptions() returned: nil, expected: %s", errTopicOrQueue)
	}
}

func TestValidateReadOptions_TopicSubscription(t *testing.T) {
	opts := &options.Options{Azure: &options.AzureServiceBusOptions{
		Topic: "test-queue",
	}}

	if err := validateReadOptions(opts); err == nil {
		t.Errorf("validateReadOptions() returned: nil, expected: %s", errMissingSubscription)
	}

	opts.Azure.Subscription = "test-sub"
	if err := validateReadOptions(opts); err != nil {
		t.Errorf("validateReadOptions() returned: %s, expected: nil", errMissingSubscription)
	}
}
