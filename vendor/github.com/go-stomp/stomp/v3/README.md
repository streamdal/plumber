# stomp

Go language implementation of a STOMP client library.

![Build Status](https://github.com/go-stomp/stomp/actions/workflows/test.yml/badge.svg?branch=master)
[![Go Reference](https://pkg.go.dev/badge/github.com/go-stomp/stomp/v3.svg)](https://pkg.go.dev/github.com/go-stomp/stomp/v3)

Features:

* Supports STOMP Specifications Versions 1.0, 1.1, 1.2 (https://stomp.github.io/)
* Protocol negotiation to select the latest mutually supported protocol
* Heart beating for testing the underlying network connection
* Tested against RabbitMQ v3.0.1

## Usage Instructions

```
go get github.com/go-stomp/stomp/v3
```

For API documentation, see https://pkg.go.dev/github.com/go-stomp/stomp/v3


Breaking changes between this previous version and the current version are 
documented in [breaking_changes.md](breaking_changes.md).


## License
Copyright 2012 - Present The go-stomp authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

