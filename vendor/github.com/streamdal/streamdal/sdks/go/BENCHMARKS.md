# WASM Benchmarks

***Up to date as of 2023-10-30***

All benchmarks include marshal/unmarshal of `WASMRequest` and `WASMResponse` respectively

*inferschema*

```bash
go test -bench=.

goos: darwin
goarch: arm64
pkg: github.com/streamdal/go-sdk
BenchmarkInferSchema_FreshSchema/small.json-8   	   15450	     77007 ns/op
BenchmarkInferSchema_FreshSchema/medium.json-8  	    2151	    554466 ns/op
BenchmarkInferSchema_FreshSchema/large.json-8   	     256	   4670424 ns/op
BenchmarkInferSchema_MatchExisting/small.json-8 	   13224	     89757 ns/op
BenchmarkInferSchema_MatchExisting/medium.json-8         	    2082	    567770 ns/op
BenchmarkInferSchema_MatchExisting/large.json-8          	     254	   4711179 ns/op
```

*transform*

```bash
go test -bench=.

goos: darwin
goarch: arm64
pkg: github.com/streamdal/go-sdk
BenchmarkTransform_Replace/small.json-8         	  110710	     10787 ns/op
BenchmarkTransform_Replace/medium.json-8        	   15782	     77025 ns/op
BenchmarkTransform_Replace/large.json-8         	    1756	    671757 ns/op
```

*search/match field values*

```bash
go test -bench=.

goos: darwin
goarch: arm64
pkg: github.com/streamdal/go-sdk
BenchmarkDetective/small.json-8                          	  249192	      4647 ns/op
BenchmarkDetective/medium.json-8                         	   67258	     18027 ns/op
BenchmarkDetective/large.json-8                          	    7782	    136151 ns/op
```
