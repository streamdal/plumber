// Code generated by counterfeiter. DO NOT EDIT.
package rabbitfakes

import (
	"context"
	"sync"

	"github.com/batchcorp/rabbit"
	"github.com/streadway/amqp"
)

type FakeIRabbit struct {
	CloseStub        func() error
	closeMutex       sync.RWMutex
	closeArgsForCall []struct {
	}
	closeReturns struct {
		result1 error
	}
	closeReturnsOnCall map[int]struct {
		result1 error
	}
	ConsumeStub        func(context.Context, chan *rabbit.ConsumeError, func(msg amqp.Delivery) error)
	consumeMutex       sync.RWMutex
	consumeArgsForCall []struct {
		arg1 context.Context
		arg2 chan *rabbit.ConsumeError
		arg3 func(msg amqp.Delivery) error
	}
	ConsumeOnceStub        func(context.Context, func(msg amqp.Delivery) error) error
	consumeOnceMutex       sync.RWMutex
	consumeOnceArgsForCall []struct {
		arg1 context.Context
		arg2 func(msg amqp.Delivery) error
	}
	consumeOnceReturns struct {
		result1 error
	}
	consumeOnceReturnsOnCall map[int]struct {
		result1 error
	}
	PublishStub        func(context.Context, string, []byte) error
	publishMutex       sync.RWMutex
	publishArgsForCall []struct {
		arg1 context.Context
		arg2 string
		arg3 []byte
	}
	publishReturns struct {
		result1 error
	}
	publishReturnsOnCall map[int]struct {
		result1 error
	}
	StopStub        func() error
	stopMutex       sync.RWMutex
	stopArgsForCall []struct {
	}
	stopReturns struct {
		result1 error
	}
	stopReturnsOnCall map[int]struct {
		result1 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeIRabbit) Close() error {
	fake.closeMutex.Lock()
	ret, specificReturn := fake.closeReturnsOnCall[len(fake.closeArgsForCall)]
	fake.closeArgsForCall = append(fake.closeArgsForCall, struct {
	}{})
	stub := fake.CloseStub
	fakeReturns := fake.closeReturns
	fake.recordInvocation("Close", []interface{}{})
	fake.closeMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeIRabbit) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *FakeIRabbit) CloseCalls(stub func() error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *FakeIRabbit) CloseReturns(result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	fake.closeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeIRabbit) CloseReturnsOnCall(i int, result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	if fake.closeReturnsOnCall == nil {
		fake.closeReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.closeReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeIRabbit) Consume(arg1 context.Context, arg2 chan *rabbit.ConsumeError, arg3 func(msg amqp.Delivery) error) {
	fake.consumeMutex.Lock()
	fake.consumeArgsForCall = append(fake.consumeArgsForCall, struct {
		arg1 context.Context
		arg2 chan *rabbit.ConsumeError
		arg3 func(msg amqp.Delivery) error
	}{arg1, arg2, arg3})
	stub := fake.ConsumeStub
	fake.recordInvocation("Consume", []interface{}{arg1, arg2, arg3})
	fake.consumeMutex.Unlock()
	if stub != nil {
		fake.ConsumeStub(arg1, arg2, arg3)
	}
}

func (fake *FakeIRabbit) ConsumeCallCount() int {
	fake.consumeMutex.RLock()
	defer fake.consumeMutex.RUnlock()
	return len(fake.consumeArgsForCall)
}

func (fake *FakeIRabbit) ConsumeCalls(stub func(context.Context, chan *rabbit.ConsumeError, func(msg amqp.Delivery) error)) {
	fake.consumeMutex.Lock()
	defer fake.consumeMutex.Unlock()
	fake.ConsumeStub = stub
}

func (fake *FakeIRabbit) ConsumeArgsForCall(i int) (context.Context, chan *rabbit.ConsumeError, func(msg amqp.Delivery) error) {
	fake.consumeMutex.RLock()
	defer fake.consumeMutex.RUnlock()
	argsForCall := fake.consumeArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeIRabbit) ConsumeOnce(arg1 context.Context, arg2 func(msg amqp.Delivery) error) error {
	fake.consumeOnceMutex.Lock()
	ret, specificReturn := fake.consumeOnceReturnsOnCall[len(fake.consumeOnceArgsForCall)]
	fake.consumeOnceArgsForCall = append(fake.consumeOnceArgsForCall, struct {
		arg1 context.Context
		arg2 func(msg amqp.Delivery) error
	}{arg1, arg2})
	stub := fake.ConsumeOnceStub
	fakeReturns := fake.consumeOnceReturns
	fake.recordInvocation("ConsumeOnce", []interface{}{arg1, arg2})
	fake.consumeOnceMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeIRabbit) ConsumeOnceCallCount() int {
	fake.consumeOnceMutex.RLock()
	defer fake.consumeOnceMutex.RUnlock()
	return len(fake.consumeOnceArgsForCall)
}

func (fake *FakeIRabbit) ConsumeOnceCalls(stub func(context.Context, func(msg amqp.Delivery) error) error) {
	fake.consumeOnceMutex.Lock()
	defer fake.consumeOnceMutex.Unlock()
	fake.ConsumeOnceStub = stub
}

func (fake *FakeIRabbit) ConsumeOnceArgsForCall(i int) (context.Context, func(msg amqp.Delivery) error) {
	fake.consumeOnceMutex.RLock()
	defer fake.consumeOnceMutex.RUnlock()
	argsForCall := fake.consumeOnceArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *FakeIRabbit) ConsumeOnceReturns(result1 error) {
	fake.consumeOnceMutex.Lock()
	defer fake.consumeOnceMutex.Unlock()
	fake.ConsumeOnceStub = nil
	fake.consumeOnceReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeIRabbit) ConsumeOnceReturnsOnCall(i int, result1 error) {
	fake.consumeOnceMutex.Lock()
	defer fake.consumeOnceMutex.Unlock()
	fake.ConsumeOnceStub = nil
	if fake.consumeOnceReturnsOnCall == nil {
		fake.consumeOnceReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.consumeOnceReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeIRabbit) Publish(arg1 context.Context, arg2 string, arg3 []byte) error {
	var arg3Copy []byte
	if arg3 != nil {
		arg3Copy = make([]byte, len(arg3))
		copy(arg3Copy, arg3)
	}
	fake.publishMutex.Lock()
	ret, specificReturn := fake.publishReturnsOnCall[len(fake.publishArgsForCall)]
	fake.publishArgsForCall = append(fake.publishArgsForCall, struct {
		arg1 context.Context
		arg2 string
		arg3 []byte
	}{arg1, arg2, arg3Copy})
	stub := fake.PublishStub
	fakeReturns := fake.publishReturns
	fake.recordInvocation("Publish", []interface{}{arg1, arg2, arg3Copy})
	fake.publishMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeIRabbit) PublishCallCount() int {
	fake.publishMutex.RLock()
	defer fake.publishMutex.RUnlock()
	return len(fake.publishArgsForCall)
}

func (fake *FakeIRabbit) PublishCalls(stub func(context.Context, string, []byte) error) {
	fake.publishMutex.Lock()
	defer fake.publishMutex.Unlock()
	fake.PublishStub = stub
}

func (fake *FakeIRabbit) PublishArgsForCall(i int) (context.Context, string, []byte) {
	fake.publishMutex.RLock()
	defer fake.publishMutex.RUnlock()
	argsForCall := fake.publishArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeIRabbit) PublishReturns(result1 error) {
	fake.publishMutex.Lock()
	defer fake.publishMutex.Unlock()
	fake.PublishStub = nil
	fake.publishReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeIRabbit) PublishReturnsOnCall(i int, result1 error) {
	fake.publishMutex.Lock()
	defer fake.publishMutex.Unlock()
	fake.PublishStub = nil
	if fake.publishReturnsOnCall == nil {
		fake.publishReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.publishReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeIRabbit) Stop() error {
	fake.stopMutex.Lock()
	ret, specificReturn := fake.stopReturnsOnCall[len(fake.stopArgsForCall)]
	fake.stopArgsForCall = append(fake.stopArgsForCall, struct {
	}{})
	stub := fake.StopStub
	fakeReturns := fake.stopReturns
	fake.recordInvocation("Stop", []interface{}{})
	fake.stopMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeIRabbit) StopCallCount() int {
	fake.stopMutex.RLock()
	defer fake.stopMutex.RUnlock()
	return len(fake.stopArgsForCall)
}

func (fake *FakeIRabbit) StopCalls(stub func() error) {
	fake.stopMutex.Lock()
	defer fake.stopMutex.Unlock()
	fake.StopStub = stub
}

func (fake *FakeIRabbit) StopReturns(result1 error) {
	fake.stopMutex.Lock()
	defer fake.stopMutex.Unlock()
	fake.StopStub = nil
	fake.stopReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeIRabbit) StopReturnsOnCall(i int, result1 error) {
	fake.stopMutex.Lock()
	defer fake.stopMutex.Unlock()
	fake.StopStub = nil
	if fake.stopReturnsOnCall == nil {
		fake.stopReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.stopReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeIRabbit) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.consumeMutex.RLock()
	defer fake.consumeMutex.RUnlock()
	fake.consumeOnceMutex.RLock()
	defer fake.consumeOnceMutex.RUnlock()
	fake.publishMutex.RLock()
	defer fake.publishMutex.RUnlock()
	fake.stopMutex.RLock()
	defer fake.stopMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeIRabbit) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ rabbit.IRabbit = new(FakeIRabbit)
