// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azservicebus

import (
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/internal/exported"
)

// Code is an error code, usable by consuming code to work with
// programatically.
type Code = exported.Code

const (
	// CodeConnectionLost means our connection was lost and all retry attempts failed.
	// This typically reflects an extended outage or connection disruption and may
	// require manual intervention.
	CodeConnectionLost = exported.CodeConnectionLost

	// CodeLockLost means that the lock token you have for a message has expired.
	// This message will be available again after the lock period expires, or, potentially
	// go to the dead letter queue if delivery attempts have been exceeded.
	CodeLockLost = exported.CodeLockLost
)

// Error represents a Service Bus specific error.
// NOTE: the Code is considered part of the published API but the message that
// comes back from Error(), as well as the underlying wrapped error, are NOT and
// are subject to change.
type Error = exported.Error
