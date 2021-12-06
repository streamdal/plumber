package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

var (
	typeFlag   *string
	outputFlag *string

	validTypes  = []string{"env"}
	validOutput = []string{"markdown"}

	kongBaseRe    *regexp.Regexp
	kongOptionsRe *regexp.Regexp
	backendRe     *regexp.Regexp
)

func init() {
	if err := parseFlags(); err != nil {
		log.Fatalf("error: %s", err)
	}

	var err error

	kongBaseRe, err = regexp.Compile(`kong:"(.+)"`)
	if err != nil {
		log.Fatalf("unable to compile kong base regex: %s", err)
	}

	kongOptionsRe, err = regexp.Compile(`([a-zA-Z0-9]+)='?([a-zA-Z0-9\-()/\\_><=:$. ]+)?'?|(required)`)
	if err != nil {
		log.Fatalf("unable to compile kong begin regex: %s", err)
	}

	backendRe, err = regexp.Compile("^vendor.+/args/ps_args_(.+).pb.go$")
	if err != nil {
		log.Fatalf("unable to compile backend regex: %s", err)
	}
}

func main() {
	switch *typeFlag {
	case "env":
		generateEnvDocs(*outputFlag)
	default:
		log.Fatalf("unknown cmd '%s'", *typeFlag)
	}
}

func generateEnvDocs(outputType string) {
	gotags, err := getGoTags()
	if err != nil {
		log.Fatalf("unable to fetch gotags: %s", err)
	}

	if len(gotags) == 0 {
		log.Fatal("unexpected: got 0 tags")
	}

	displayServerEnvs(outputType, gotags)
}

func displayServerEnvs(outputType string, gotags []*GrepResult) {
	if outputType != "markdown" {
		log.Fatalf("'%s' output type is not supported", outputType)
	}

	sorted := organizeTags(gotags)

	fmt.Printf("# Available environment variables\n\n")
	fmt.Printf("## Server\n\n")

	displayMarkdown(sorted.Server)

	fmt.Printf("\n## Relay\n\n")

	displayMarkdown(sorted.Relay)

	// Deterministic order (so we don't create git changes every time we generate)
	keys := make([]string, 0)

	for k := range sorted.Backends {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	fmt.Printf("\n## Backends\n\n")

	for _, key := range keys {
		fmt.Printf("### %s\n\n", strings.Title(key))
		displayMarkdown(sorted.Backends[key])

		fmt.Printf("\n")
	}
}

func displayMarkdown(tags []*KongTag) {
	fmt.Printf("| **Environment Variable** | **Description** | **Default** | **Required** |\n")
	fmt.Printf("| ------------------------ | --------------- | ----------- | ------------ |\n")

	for _, v := range tags {
		requiredStr := fmt.Sprint(v.Required)

		if v.Required {
			requiredStr = "**" + requiredStr + "**"
		}

		fmt.Printf("| %s | %s | %s | %v |\n", v.Var, v.Help, v.Default, requiredStr)
	}
}

type Sorted struct {
	Server   []*KongTag
	Relay    []*KongTag
	Backends map[string][]*KongTag
}

func organizeTags(gotags []*GrepResult) *Sorted {
	sorted := &Sorted{
		Server:   make([]*KongTag, 0),
		Relay:    make([]*KongTag, 0),
		Backends: make(map[string][]*KongTag),
	}

	for _, v := range gotags {
		kongTag, err := parseKongTag(v.Value)
		if err != nil {
			// fmt.Println("warning: unable to parse kong tag: ", err)
			continue
		}

		// Skip if option exposes an env var
		if kongTag.Var == "" {
			continue
		}

		if strings.Contains(v.Path, "opts_server.pb.go") {
			sorted.Server = append(sorted.Server, kongTag)
		} else if strings.Contains(v.Path, "opts_relay.pb.go") {
			sorted.Relay = append(sorted.Relay, kongTag)
		} else {
			match := backendRe.FindStringSubmatch(v.Path)
			if len(match) != 2 {
				continue
			}

			backend := match[1]

			if _, ok := sorted.Backends[backend]; !ok {
				sorted.Backends[backend] = make([]*KongTag, 0)
			}

			sorted.Backends[backend] = append(sorted.Backends[backend], kongTag)
		}
	}

	return sorted
}

// Doing regex because writing a tokenizer is too much effort
func parseKongTag(s string) (*KongTag, error) {
	tags := kongBaseRe.FindStringSubmatch(s)

	if len(tags) != 2 {
		return nil, errors.New("string does not contain kong tags")
	}

	options := kongOptionsRe.FindAllStringSubmatch(tags[1], -1)
	kongTag := &KongTag{}

	for _, group := range options {
		numElements := len(group)

		if numElements < 2 {
			continue
		}

		// If length is 3, option has an assignment
		// If length is 2, option has no assignment

		if group[0] == "required" {
			kongTag.Required = true
			continue
		}

		// It's an option with an assignment - should have 3 elements
		if numElements != 4 {
			fmt.Printf("warning: expected 3 elements but got '%d'\n", numElements)
			continue
		}

		// 0: original capture
		// 1: option name
		// 2: option value
		// 3: .. blank? Something funky in the regex. Ignore.

		switch group[1] {
		case "":
			continue
		case "help":
			kongTag.Help = group[2]
		case "env":
			kongTag.Var = group[2]
		case "default":
			kongTag.Default = group[2]
		default:
			// fmt.Printf("unknown option '%s' - skipping\n", group[1])
			continue
		}
	}

	return kongTag, nil
}

type KongTag struct {
	Var      string
	Help     string
	Default  string
	Required bool
}

type GrepResult struct {
	Path  string
	Value string
}

func getGoTags() ([]*GrepResult, error) {
	// /usr/bin/grep -ER 'gotags:.+' vendor/github.com/batchcorp/plumber-schemas/build/go/protos

	grepArgs := []string{"-ER", `gotags:.+`, "vendor/github.com/batchcorp/plumber-schemas/build/go/protos"}

	out, err := exec.Command("grep", grepArgs...).CombinedOutput()
	if err != nil {
		return nil, errors.Wrap(err, "unable to exec grep cmd")
	}

	results := make([]*GrepResult, 0)

	regex, err := regexp.Compile(`(.+\.pb\.go):\s+(.+)`)
	if err != nil {
		return nil, errors.Wrap(err, "regex does not compile")
	}

	for _, line := range strings.Split(string(out), "\n") {
		if line == "" {
			continue
		}

		found := regex.FindStringSubmatch(line)

		if len(found) != 3 {
			fmt.Printf("regex produced unexpected number of results (got %d; expected 3) for line: %s\n", len(found), line)
			continue
		}

		results = append(results, &GrepResult{
			Path:  found[1],
			Value: found[2],
		})
	}

	return results, nil
}

func parseFlags() error {
	typeFlag = flag.String("type", "env", "What type of docs to generate (options: env)")
	outputFlag = flag.String("output", "markdown", "What format to use for output (options: markdown)")

	flag.Parse()

	if typeFlag == nil || outputFlag == nil {
		return errors.New("usage: ./doc-generator [-h] ...")
	}

	var validTypeFlag bool

	for _, v := range validTypes {
		if v == *typeFlag {
			validTypeFlag = true
		}
	}

	if !validTypeFlag {
		return fmt.Errorf("'%s' is an invalid -type", *typeFlag)
	}

	var validOutputFlag bool

	for _, v := range validOutput {
		if v == *outputFlag {
			validOutputFlag = true
		}
	}

	if !validOutputFlag {
		return fmt.Errorf("'%s' is an invalid -output", *outputFlag)
	}

	return nil
}
