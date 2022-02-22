// Code generated by counterfeiter. DO NOT EDIT.
package etcdfakes

import (
	"context"
	"sync"

	"github.com/batchcorp/grpc-collector/backends/etcd"
	"go.etcd.io/etcd/clientv3"
)

type FakeIEtcd struct {
	DeleteStub        func(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	deleteMutex       sync.RWMutex
	deleteArgsForCall []struct {
		arg1 context.Context
		arg2 string
		arg3 []clientv3.OpOption
	}
	deleteReturns struct {
		result1 *clientv3.DeleteResponse
		result2 error
	}
	deleteReturnsOnCall map[int]struct {
		result1 *clientv3.DeleteResponse
		result2 error
	}
	GetStub        func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
	getMutex       sync.RWMutex
	getArgsForCall []struct {
		arg1 context.Context
		arg2 string
		arg3 []clientv3.OpOption
	}
	getReturns struct {
		result1 *clientv3.GetResponse
		result2 error
	}
	getReturnsOnCall map[int]struct {
		result1 *clientv3.GetResponse
		result2 error
	}
	PutStub        func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)
	putMutex       sync.RWMutex
	putArgsForCall []struct {
		arg1 context.Context
		arg2 string
		arg3 string
		arg4 []clientv3.OpOption
	}
	putReturns struct {
		result1 *clientv3.PutResponse
		result2 error
	}
	putReturnsOnCall map[int]struct {
		result1 *clientv3.PutResponse
		result2 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeIEtcd) Delete(arg1 context.Context, arg2 string, arg3 ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	fake.deleteMutex.Lock()
	ret, specificReturn := fake.deleteReturnsOnCall[len(fake.deleteArgsForCall)]
	fake.deleteArgsForCall = append(fake.deleteArgsForCall, struct {
		arg1 context.Context
		arg2 string
		arg3 []clientv3.OpOption
	}{arg1, arg2, arg3})
	stub := fake.DeleteStub
	fakeReturns := fake.deleteReturns
	fake.recordInvocation("Delete", []interface{}{arg1, arg2, arg3})
	fake.deleteMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3...)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeIEtcd) DeleteCallCount() int {
	fake.deleteMutex.RLock()
	defer fake.deleteMutex.RUnlock()
	return len(fake.deleteArgsForCall)
}

func (fake *FakeIEtcd) DeleteCalls(stub func(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error)) {
	fake.deleteMutex.Lock()
	defer fake.deleteMutex.Unlock()
	fake.DeleteStub = stub
}

func (fake *FakeIEtcd) DeleteArgsForCall(i int) (context.Context, string, []clientv3.OpOption) {
	fake.deleteMutex.RLock()
	defer fake.deleteMutex.RUnlock()
	argsForCall := fake.deleteArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeIEtcd) DeleteReturns(result1 *clientv3.DeleteResponse, result2 error) {
	fake.deleteMutex.Lock()
	defer fake.deleteMutex.Unlock()
	fake.DeleteStub = nil
	fake.deleteReturns = struct {
		result1 *clientv3.DeleteResponse
		result2 error
	}{result1, result2}
}

func (fake *FakeIEtcd) DeleteReturnsOnCall(i int, result1 *clientv3.DeleteResponse, result2 error) {
	fake.deleteMutex.Lock()
	defer fake.deleteMutex.Unlock()
	fake.DeleteStub = nil
	if fake.deleteReturnsOnCall == nil {
		fake.deleteReturnsOnCall = make(map[int]struct {
			result1 *clientv3.DeleteResponse
			result2 error
		})
	}
	fake.deleteReturnsOnCall[i] = struct {
		result1 *clientv3.DeleteResponse
		result2 error
	}{result1, result2}
}

func (fake *FakeIEtcd) Get(arg1 context.Context, arg2 string, arg3 ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	fake.getMutex.Lock()
	ret, specificReturn := fake.getReturnsOnCall[len(fake.getArgsForCall)]
	fake.getArgsForCall = append(fake.getArgsForCall, struct {
		arg1 context.Context
		arg2 string
		arg3 []clientv3.OpOption
	}{arg1, arg2, arg3})
	stub := fake.GetStub
	fakeReturns := fake.getReturns
	fake.recordInvocation("Get", []interface{}{arg1, arg2, arg3})
	fake.getMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3...)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeIEtcd) GetCallCount() int {
	fake.getMutex.RLock()
	defer fake.getMutex.RUnlock()
	return len(fake.getArgsForCall)
}

func (fake *FakeIEtcd) GetCalls(stub func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)) {
	fake.getMutex.Lock()
	defer fake.getMutex.Unlock()
	fake.GetStub = stub
}

func (fake *FakeIEtcd) GetArgsForCall(i int) (context.Context, string, []clientv3.OpOption) {
	fake.getMutex.RLock()
	defer fake.getMutex.RUnlock()
	argsForCall := fake.getArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FakeIEtcd) GetReturns(result1 *clientv3.GetResponse, result2 error) {
	fake.getMutex.Lock()
	defer fake.getMutex.Unlock()
	fake.GetStub = nil
	fake.getReturns = struct {
		result1 *clientv3.GetResponse
		result2 error
	}{result1, result2}
}

func (fake *FakeIEtcd) GetReturnsOnCall(i int, result1 *clientv3.GetResponse, result2 error) {
	fake.getMutex.Lock()
	defer fake.getMutex.Unlock()
	fake.GetStub = nil
	if fake.getReturnsOnCall == nil {
		fake.getReturnsOnCall = make(map[int]struct {
			result1 *clientv3.GetResponse
			result2 error
		})
	}
	fake.getReturnsOnCall[i] = struct {
		result1 *clientv3.GetResponse
		result2 error
	}{result1, result2}
}

func (fake *FakeIEtcd) Put(arg1 context.Context, arg2 string, arg3 string, arg4 ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	fake.putMutex.Lock()
	ret, specificReturn := fake.putReturnsOnCall[len(fake.putArgsForCall)]
	fake.putArgsForCall = append(fake.putArgsForCall, struct {
		arg1 context.Context
		arg2 string
		arg3 string
		arg4 []clientv3.OpOption
	}{arg1, arg2, arg3, arg4})
	stub := fake.PutStub
	fakeReturns := fake.putReturns
	fake.recordInvocation("Put", []interface{}{arg1, arg2, arg3, arg4})
	fake.putMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3, arg4...)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeIEtcd) PutCallCount() int {
	fake.putMutex.RLock()
	defer fake.putMutex.RUnlock()
	return len(fake.putArgsForCall)
}

func (fake *FakeIEtcd) PutCalls(stub func(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error)) {
	fake.putMutex.Lock()
	defer fake.putMutex.Unlock()
	fake.PutStub = stub
}

func (fake *FakeIEtcd) PutArgsForCall(i int) (context.Context, string, string, []clientv3.OpOption) {
	fake.putMutex.RLock()
	defer fake.putMutex.RUnlock()
	argsForCall := fake.putArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3, argsForCall.arg4
}

func (fake *FakeIEtcd) PutReturns(result1 *clientv3.PutResponse, result2 error) {
	fake.putMutex.Lock()
	defer fake.putMutex.Unlock()
	fake.PutStub = nil
	fake.putReturns = struct {
		result1 *clientv3.PutResponse
		result2 error
	}{result1, result2}
}

func (fake *FakeIEtcd) PutReturnsOnCall(i int, result1 *clientv3.PutResponse, result2 error) {
	fake.putMutex.Lock()
	defer fake.putMutex.Unlock()
	fake.PutStub = nil
	if fake.putReturnsOnCall == nil {
		fake.putReturnsOnCall = make(map[int]struct {
			result1 *clientv3.PutResponse
			result2 error
		})
	}
	fake.putReturnsOnCall[i] = struct {
		result1 *clientv3.PutResponse
		result2 error
	}{result1, result2}
}

func (fake *FakeIEtcd) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.deleteMutex.RLock()
	defer fake.deleteMutex.RUnlock()
	fake.getMutex.RLock()
	defer fake.getMutex.RUnlock()
	fake.putMutex.RLock()
	defer fake.putMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeIEtcd) recordInvocation(key string, args []interface{}) {
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

func Start(serviceCtx context.Context) error {
	return nil
}

func Stop() error {
	return nil
}


var _ etcd.IEtcd = new(FakeIEtcd)
