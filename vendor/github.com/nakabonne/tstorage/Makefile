test:
	go test -race -v -coverpkg=./... -covermode=atomic -coverprofile=coverage.txt ./...

test-bench:
	go test -benchtime=4s -benchmem -bench=. -cpuprofile=pprof/cpu.out -memprofile=pprof/mem.out .

pprof-mem:
	go tool pprof pprof/mem.out

pprof-cpu:
	go tool pprof pprof/cpu.out

dep:
	go mod tidy

godoc:
	godoc -http=:6060
