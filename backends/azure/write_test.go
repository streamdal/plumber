package azure

import (
	"testing"

	"github.com/batchcorp/plumber/cli"
)

func TestValidateWriteOptions_Passes(t *testing.T) {
	opts := &cli.Options{Azure: &cli.AzureServiceBusOptions{
		Queue: "test-queue",
		Topic: "",
	}}

	if err := validateWriteOptions(opts); err != nil {
		t.Errorf("validateReadOptions() returned: %s, expected: nil", err)
	}
}

func TestValidateWriteOptions_Fails(t *testing.T) {
	opts := &cli.Options{Azure: &cli.AzureServiceBusOptions{
		Queue: "test-queue",
		Topic: "test-topic",
	}}

	if err := validateWriteOptions(opts); err == nil {
		t.Errorf("validateReadOptions() returned: nil, expected: %s", errTopicOrQueue)
	}
}
