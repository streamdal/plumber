# Thrifty
---
Thrifty is used to transform a wire-format thrift message into a JSON representation
using an IDL definition. This library builds upon github.com/thrift-iterator/go by utilizing
the IDL definition in order to properly represent field names and enum values in the output
instead of IDs.


## Usage

First parse the IDL:

```go
idlFiles := map[string][]byte{
	"simple.thrift": []byte(`
            namespace go sh.batch.schema
            
            struct Account {
              1: i32 id
              2: string first_name
              3: string last_name
              4: string email
            }
        `),
}


idl, err := thrifty.ParseIDLFiles(idlFiles)
if err != nil {
	log.Fatalf("unable to parse IDL files: %s", err.Error())
}
```

Then decode the binary message data using the parsed IDL. The struct name must be prefixed with the full namespace
 `"sh.batch.schema.Account"` as shown: 

```go
decodedMsg, err := thrify.DecodeWithParsedIDL(idl, msgData, "sh.batch.schema.Account")
if err != nil {
	log.Fatalf("unable to decode thrift message: %s", err.Error())
}
```

The result of decodedMsg will be JSON in `[]byte` format:

```go
println(string(decodedMsg))
```

Output:
```json
{"first_name": "Gopher", "last_name": "Golang", "email": "gopher@golang.org", "id": 348590795}
```


## Benchmarks

Single struct: 
```
struct Account {
  1: i32 id
  2: string first_name
  3: string last_name
  4: string email
}
```

```bash
goos: darwin
goarch: arm64
pkg: github.com/batchcorp/thrifty
BenchmarkParseIDL-8              	  192861	      6335 ns/op
BenchmarkDecodeWithParsedIDL-8   	  974961	      1129 ns/op
BenchmarkDecodeWithRawIDL-8      	  154863	      7734 ns/op
```

Multiple nested structs:
```
struct Account {
  1: i32 id
  2: string first_name
  3: string last_name
  4: string email
  5: Billing billing
  6: Address address
  7: Deep1 deep_nested
}

struct Billing {
  1: string card_number
  2: i32 exp_month
  3: i32 exp_year
}

struct Address {
  1: string street
  2: string city
  3: string state_province
  4: string country
  5: string postal_code
}

struct Deep1 {
  1: Deep2 deep2
}

struct Deep2 {
  1: Deep3 deep3
}

struct Deep3 {
  1: string nested_value
}
```

```bash
goos: darwin
goarch: arm64
pkg: github.com/batchcorp/thrifty
BenchmarkParseIDL_nested-8              	   51273	     23310 ns/op
BenchmarkDecodeWithParsedIDL_nested-8   	  261242	      4602 ns/op
BenchmarkDecodeWithRawIDL_nested-8      	   39578	     28945 ns/op
```