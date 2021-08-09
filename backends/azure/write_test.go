package azure

import (
	"testing"

	"github.com/batchcorp/plumber/options"
)

func TestValidateWriteOptions_Passes(t *testing.T) {
	opts := &options.Options{Azure: &options.AzureServiceBusOptions{
		Queue: "test-queue",
		Topic: "",
	}}

	if err := validateWriteOptions(opts); err != nil {
		t.Errorf("validateReadOptions() returned: %s, expected: nil", err)
	}
}

func TestValidateWriteOptions_Fails(t *testing.T) {
	opts := &options.Options{Azure: &options.AzureServiceBusOptions{
		Queue: "test-queue",
		Topic: "test-topic",
	}}

	if err := validateWriteOptions(opts); err == nil {
		t.Errorf("validateReadOptions() returned: nil, expected: %s", errTopicOrQueue)
	}
}
