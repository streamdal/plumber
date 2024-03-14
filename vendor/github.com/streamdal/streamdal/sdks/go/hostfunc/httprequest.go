package hostfunc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"github.com/tetratelabs/wazero/api"

	"github.com/streamdal/streamdal/libs/protos/build/go/protos/steps"

	"github.com/streamdal/streamdal/sdks/go/helper"
)

// HTTPRequest is function that is exported to and called from a Rust WASM module
func (h *HostFunc) HTTPRequest(_ context.Context, module api.Module, ptr, length int32) uint64 {
	request := &steps.HttpRequest{}

	if err := helper.ReadRequestFromMemory(module, request, ptr, length); err != nil {
		return httpRequestResponse(module, http.StatusInternalServerError, "unable to read HTTP request params: "+err.Error(), nil)
	}

	httpReq, err := http.NewRequest(methodFromProto(request.Method), request.Url, bytes.NewReader(request.Body))
	if err != nil {
		err = errors.Wrap(err, "unable to create http request")
		return httpRequestResponse(module, http.StatusInternalServerError, err.Error(), nil)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		err = errors.Wrap(err, "unable to perform http request")
		return httpRequestResponse(module, http.StatusInternalServerError, err.Error(), nil)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return httpRequestResponse(module, http.StatusInternalServerError, err.Error(), nil)
	}

	if resp.StatusCode > 299 {
		return httpRequestResponse(module, resp.StatusCode, string(body), nil)
	}

	// Get all headers from the response
	headers := make(map[string]string)
	for k, v := range resp.Header {
		headers[k] = strings.Join(v, ", ")
	}

	return httpRequestResponse(module, resp.StatusCode, string(body), headers)
}

// httpRequestResponse is a helper for HostFuncHTTPRequest()
func httpRequestResponse(module api.Module, code int, body string, headers map[string]string) uint64 {
	if headers == nil {
		headers = make(map[string]string)
	}

	resp := &steps.HttpResponse{
		Code:    int32(code),
		Body:    []byte(body),
		Headers: headers,
	}

	addr, err := helper.WriteResponseToMemory(module, resp)
	if err != nil {
		panic("unable to write HTTP response to memory: " + err.Error())
	}

	return addr
}

func methodFromProto(m steps.HttpRequestMethod) string {
	switch m {
	case steps.HttpRequestMethod_HTTP_REQUEST_METHOD_POST:
		return http.MethodPost
	case steps.HttpRequestMethod_HTTP_REQUEST_METHOD_PUT:
		return http.MethodPut
	case steps.HttpRequestMethod_HTTP_REQUEST_METHOD_DELETE:
		return http.MethodDelete
	case steps.HttpRequestMethod_HTTP_REQUEST_METHOD_PATCH:
		return http.MethodPatch
	case steps.HttpRequestMethod_HTTP_REQUEST_METHOD_HEAD:
		return http.MethodHead
	case steps.HttpRequestMethod_HTTP_REQUEST_METHOD_OPTIONS:
		return http.MethodOptions
	default:
		return http.MethodGet
	}
}
