package hostfunc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/tetratelabs/wazero/api"

	"github.com/streamdal/streamdal/libs/protos/build/go/protos"
	"github.com/streamdal/streamdal/libs/protos/build/go/protos/steps"
	"github.com/streamdal/streamdal/sdks/go/helper"
)

// HTTPRequest is function that is exported to and called from a Rust WASM module
func (h *HostFunc) HTTPRequest(_ context.Context, module api.Module, ptr, length int32) uint64 {
	request := &protos.WASMRequest{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := helper.ReadRequestFromMemory(module, request, ptr, length); err != nil {
		return httpRequestResponse(module, http.StatusInternalServerError, "unable to read HTTP request params: "+err.Error(), nil)
	}

	req := request.Step.GetHttpRequest().Request

	reqBody, err := getRequestBodyForMode(request)
	if err != nil {
		return httpRequestResponse(module, http.StatusInternalServerError, err.Error(), nil)
	}

	httpReq, err := http.NewRequestWithContext(ctx, methodFromProto(req.Method), req.Url, reqBody)
	if err != nil {
		err = errors.Wrap(err, "unable to create http request")
		return httpRequestResponse(module, http.StatusInternalServerError, err.Error(), nil)
	}

	// Set headers
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	// Default content-type to JSON if not set and body is probably JSON
	if len(req.Body) > 0 && req.Body[0] == '{' && httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/json")
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

// getRequestBodyForMode returns the request body for the given mode
// If mode == static, then it returns the body the user specified
// If mode == inter step result, then it returns the inter step result as JSON
func getRequestBodyForMode(request *protos.WASMRequest) (io.Reader, error) {
	httpReq := request.Step.GetHttpRequest().Request

	switch httpReq.BodyMode {
	case steps.HttpRequestBodyMode_HTTP_REQUEST_BODY_MODE_STATIC:
		return bytes.NewReader(httpReq.Body), nil
	case steps.HttpRequestBodyMode_HTTP_REQUEST_BODY_MODE_INTER_STEP_RESULT:
		if request.InterStepResult == nil {
			return nil, errors.New("inter step result is empty")
		}

		detectiveRes := request.InterStepResult.GetDetectiveResult()
		if detectiveRes == nil {
			return nil, errors.New("no detective result found in inter step result")
		}

		for _, match := range detectiveRes.Matches {
			// Clear value from match
			match.Value = nil
		}

		m := jsonpb.Marshaler{
			EnumsAsInts:  false,
			EmitDefaults: true,
			OrigName:     true,
		}

		data, err := m.MarshalToString(request.InterStepResult)
		if err != nil {
			return nil, errors.Wrap(err, "unable to marshal inter step result to string")
		}

		return strings.NewReader(data), nil

	}

	return nil, nil
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
