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

package auth

import (
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"

	zms "github.com/AthenZ/athenz/libs/go/zmssvctoken"
	zts "github.com/AthenZ/athenz/libs/go/ztsroletoken"
)

const (
	minExpire            = 2 * time.Hour
	maxExpire            = 24 * time.Hour
	AthenzRoleAuthHeader = "Athenz-Role-Auth"
)

type athenzAuthProvider struct {
	providerDomain     string
	tenantDomain       string
	tenantService      string
	privateKey         string
	keyID              string
	principalHeader    string
	ztsURL             string
	tokenBuilder       zms.TokenBuilder
	roleToken          zts.RoleToken
	zmsNewTokenBuilder func(domain, name string, privateKeyPEM []byte, keyVersion string) (zms.TokenBuilder, error)
	ztsNewRoleToken    func(tok zms.Token, domain string, opts zts.RoleTokenOptions) zts.RoleToken

	T http.RoundTripper
}

type privateKeyURI struct {
	Scheme                   string
	MediaTypeAndEncodingType string
	Data                     string
	Path                     string
}

func NewAuthenticationAthenzWithParams(params map[string]string) (Provider, error) {
	return NewAuthenticationAthenz(
		params["providerDomain"],
		params["tenantDomain"],
		params["tenantService"],
		params["privateKey"],
		params["keyId"],
		params["principalHeader"],
		params["ztsUrl"],
	), nil
}

func NewAuthenticationAthenz(
	providerDomain string,
	tenantDomain string,
	tenantService string,
	privateKey string,
	keyID string,
	principalHeader string,
	ztsURL string) Provider {
	var fixedKeyID string
	if keyID == "" {
		fixedKeyID = "0"
	} else {
		fixedKeyID = keyID
	}
	ztsNewRoleToken := func(tok zms.Token, domain string, opts zts.RoleTokenOptions) zts.RoleToken {
		return zts.RoleToken(zts.NewRoleToken(tok, domain, opts))
	}

	return &athenzAuthProvider{
		providerDomain:     providerDomain,
		tenantDomain:       tenantDomain,
		tenantService:      tenantService,
		privateKey:         privateKey,
		keyID:              fixedKeyID,
		principalHeader:    principalHeader,
		ztsURL:             strings.TrimSuffix(ztsURL, "/"),
		zmsNewTokenBuilder: zms.NewTokenBuilder,
		ztsNewRoleToken:    ztsNewRoleToken,
	}
}

func (p *athenzAuthProvider) Init() error {
	uriSt := parseURI(p.privateKey)
	var keyData []byte

	if uriSt.Scheme == "data" {
		if uriSt.MediaTypeAndEncodingType != "application/x-pem-file;base64" {
			return errors.New("Unsupported mediaType or encodingType: " + uriSt.MediaTypeAndEncodingType)
		}
		key, err := base64.StdEncoding.DecodeString(uriSt.Data)
		if err != nil {
			return err
		}
		keyData = key
	} else if uriSt.Scheme == "file" {
		key, err := ioutil.ReadFile(uriSt.Path)
		if err != nil {
			return err
		}
		keyData = key
	} else {
		return errors.New("Unsupported URI Scheme: " + uriSt.Scheme)
	}

	tb, err := p.zmsNewTokenBuilder(p.tenantDomain, p.tenantService, keyData, p.keyID)
	if err != nil {
		return err
	}
	p.tokenBuilder = tb

	roleToken := p.ztsNewRoleToken(p.tokenBuilder.Token(), p.providerDomain, zts.RoleTokenOptions{
		BaseZTSURL: p.ztsURL + "/zts/v1",
		MinExpire:  minExpire,
		MaxExpire:  maxExpire,
		AuthHeader: p.principalHeader,
	})
	p.roleToken = roleToken

	return nil
}

func (p *athenzAuthProvider) Name() string {
	return "athenz"
}

func (p *athenzAuthProvider) GetTLSCertificate() (*tls.Certificate, error) {
	return nil, nil
}

func (p *athenzAuthProvider) GetData() ([]byte, error) {
	tok, err := p.roleToken.RoleTokenValue()
	if err != nil {
		return nil, err
	}

	return []byte(tok), nil
}

func (p *athenzAuthProvider) Close() error {
	return nil
}

func parseURI(uri string) privateKeyURI {
	var uriSt privateKeyURI
	// scheme mediatype[;base64] path file
	const expression = `^(?:([^:/?#]+):)(?:([;/\\\-\w]*),)?(?:/{0,2}((?:[^?#/]*/)*))?([^?#]*)`

	// when expression cannot be parsed, then panics
	re := regexp.MustCompile(expression)
	if re.MatchString(uri) {
		groups := re.FindStringSubmatch(uri)
		uriSt.Scheme = groups[1]
		uriSt.MediaTypeAndEncodingType = groups[2]
		uriSt.Data = groups[4]
		uriSt.Path = groups[3] + groups[4]
	}

	return uriSt
}

func (p *athenzAuthProvider) RoundTrip(req *http.Request) (*http.Response, error) {
	tok, err := p.roleToken.RoleTokenValue()
	if err != nil {
		return nil, err
	}
	req.Header.Add(AthenzRoleAuthHeader, tok)
	return p.T.RoundTrip(req)
}

func (p *athenzAuthProvider) Transport() http.RoundTripper {
	return p.T
}

func (p *athenzAuthProvider) WithTransport(tripper http.RoundTripper) error {
	p.T = tripper
	return nil
}
