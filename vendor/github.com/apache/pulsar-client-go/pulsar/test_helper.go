// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
	"github.com/stretchr/testify/assert"

	pkgerrors "github.com/pkg/errors"
)

const (
	serviceURL    = "pulsar://localhost:6650"
	serviceURLTLS = "pulsar+ssl://localhost:6651"

	webServiceURL    = "http://localhost:8080"
	webServiceURLTLS = "https://localhost:8443"

	caCertsPath       = "../integration-tests/certs/cacert.pem"
	tlsClientCertPath = "../integration-tests/certs/client-cert.pem"
	tlsClientKeyPath  = "../integration-tests/certs/client-key.pem"
	tokenFilePath     = "../integration-tests/tokens/token.txt"
)

func newTopicName() string {
	return fmt.Sprintf("my-topic-%v", time.Now().Nanosecond())
}

func newAuthTopicName() string {
	return fmt.Sprintf("private/auth/my-topic-%v", time.Now().Nanosecond())
}

func testEndpoint(parts ...string) string {
	return webServiceURL + "/" + path.Join(parts...)
}

func jsonHeaders() http.Header {
	headers := http.Header{}
	headers.Add("Content-Type", "application/json")
	headers.Add("Accept", "application/json")
	return headers
}

func httpDelete(requestPaths ...string) error {
	var errs error
	for _, requestPath := range requestPaths {
		if err := httpDo(http.MethodDelete, requestPath, nil, nil); err != nil {
			errs = pkgerrors.Wrapf(err, "unable to delete url: %s"+requestPath)
		}
	}
	return errs
}

func httpPut(requestPath string, body interface{}) error {
	return httpDo(http.MethodPut, requestPath, body, nil)
}

func httpGet(requestPath string, out interface{}) error {
	return httpDo(http.MethodGet, requestPath, nil, out)
}

func httpDo(method string, requestPath string, in interface{}, out interface{}) error {
	client := http.DefaultClient
	endpoint := testEndpoint(requestPath)
	var body io.Reader
	inBytes, err := json.Marshal(in)
	if err != nil {
		return err
	}
	body = bytes.NewReader(inBytes)
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return err
	}

	req.Header = jsonHeaders()
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode > 299 || resp.StatusCode < 200 {
		return fmt.Errorf("http error status code: %d", resp.StatusCode)
	}

	if out != nil {
		outBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return json.Unmarshal(outBytes, out)
	}

	return nil
}

func makeHTTPCall(t *testing.T, method string, url string, body string) {
	client := http.Client{}

	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Body != nil {
		_ = res.Body.Close()
	}
}

func createNamespace(namespace string, policy map[string]interface{}) error {
	return httpPut("admin/v2/namespaces/"+namespace, policy)
}

func createTopic(topic string) error {
	return httpPut("admin/v2/persistent/"+topic, nil)
}

func deleteTopic(topic string) error {
	return httpDelete("admin/v2/persistent/" + fmt.Sprintf("%s?force=true", topic))
}

func topicStats(topic string) (map[string]interface{}, error) {
	var metadata map[string]interface{}
	err := httpGet("admin/v2/persistent/"+topicPath(topic)+"/stats", &metadata)
	return metadata, err
}

func topicPath(topic string) string {
	tn, _ := internal.ParseTopicName(topic)
	idx := strings.LastIndex(tn.Name, "/")
	if idx > 0 {
		return tn.Namespace + "/" + tn.Name[idx:]
	}
	return tn.Name
}

func retryAssert(t assert.TestingT, times int, milliseconds int, update func(), assert func(assert.TestingT) bool) {
	for i := 0; i < times; i++ {
		time.Sleep(time.Duration(milliseconds) * time.Millisecond)
		update()
		if assert(nil) {
			break
		}
	}
	assert(t)
}
