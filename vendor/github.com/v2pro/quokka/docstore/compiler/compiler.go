package compiler

import (
	"os"
	"io/ioutil"
	"os/exec"
	"plugin"
	"crypto/sha1"
	"encoding/base32"
	"runtime"
	"bytes"
	"strings"
	"strconv"
	"fmt"
	"github.com/v2pro/plz/countlog"
)

type API interface {
	DynamicCompile(source string) (*plugin.Plugin, error)
}

var ConfigDefault = Config{
	PluginCacheDir: os.Getenv("HOME") + "/.docstore_handler",
	SourceTempDir:  os.Getenv("HOME") + "/.docstore_handler",
}.Froze()

func DynamicCompile(source string) (*plugin.Plugin, error) {
	if os.Getenv("DOCSTORE_DEBUG") == "true" {
		fmt.Println(annotateLines(source))
	}
	return ConfigDefault.DynamicCompile(source)
}

type Config struct {
	PluginCacheDir string
	SourceTempDir  string
}

func (config Config) Froze() API {
	return &frozenConfig{config: config}
}

type frozenConfig struct {
	config Config
}

func (frozen *frozenConfig) DynamicCompile(source string) (*plugin.Plugin, error) {
	cfg := frozen.config
	if _, err := os.Stat(cfg.PluginCacheDir); err != nil {
		err := os.Mkdir(cfg.PluginCacheDir, 0777)
		if err != nil {
			countlog.Error(
				"event!compiler.failed to create plugin cache dir",
				"err", err,
				"dir", cfg.PluginCacheDir)
			return nil, err
		}
	}
	sourceHash := hash(source)
	srcFileName := cfg.SourceTempDir + "/" + sourceHash + ".go"
	soFileName := cfg.PluginCacheDir + "/" + sourceHash + ".so"
	if _, err := os.Stat(soFileName); err == nil {
		thePlugin, err := plugin.Open(soFileName)
		if err != nil {
			countlog.Warn(
				"event!compiler.failed to load cached plugin",
				"soFileName", soFileName)
		} else {
			if verifySourceHash(thePlugin, sourceHash) {
				countlog.Debug("event!compiler.reuse plugin", "soFileName", soFileName)
				return thePlugin, nil
			}
			countlog.Info("event!compiler.cached date plugin is out of date", "soFileName", soFileName)
		}
	}
	err := ioutil.WriteFile(srcFileName, []byte(fmt.Sprintf(`
%s
var SOURCE__HASH = "%s"
	`, source, sourceHash)), 0666)
	if err != nil {
		countlog.Error("event!compiler.failed to write source temp file",
			"err", err,
			"srcFileName", srcFileName)
		return nil, err
	}
	countlog.Debug("event!compiler.build plugin", "soFileName", soFileName, "srcFileName", srcFileName)
	cmd := exec.Command("go", "build", "-gcflags", "-N", "-buildmode=plugin", "-o", soFileName, srcFileName)
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	err = cmd.Run()
	if err != nil {
		countlog.Error(
			"event!compiler.failed to compile generated plugin",
			"err", err,
			"stdout", outBuf.String(),
			"stderr", errBuf.String(),
			"srcFileName", srcFileName,
			"source", annotateLines(source))
		return nil, err
	}
	countlog.Debug("event!compiler.open plugin", "soFileName", soFileName)
	thePlugin, err := plugin.Open(soFileName)
	if err != nil {
		countlog.Error(
			"event!compiler.failed to load generated plugin",
			"err", err,
			"soFileName", soFileName)
		return nil, err
	}
	return thePlugin, nil
}

func hash(source string) string {
	h := sha1.New()
	h.Write([]byte(source))
	h.Write([]byte(runtime.Version()))
	return "g" + base32.StdEncoding.EncodeToString(h.Sum(nil))
}

func annotateLines(source string) string {
	var buf bytes.Buffer
	lines := strings.Split(source, "\n")
	for i, line := range lines {
		lineNo := strconv.FormatInt(int64(i+1), 10)
		buf.WriteString(lineNo)
		buf.WriteString(": ")
		buf.WriteString(line)
		buf.WriteString("\n")
	}
	return buf.String()
}

func verifySourceHash(thePlugin *plugin.Plugin, sourceHash string) bool {
	symbol, err := thePlugin.Lookup("SOURCE__HASH")
	if err != nil {
		countlog.Error("event!compiler.SOURCE__HASH missing from so", "err", err)
		return false
	}
	actualSourceHash, isStr := symbol.(*string)
	if !isStr {
		countlog.Error("event!compiler.SOURCE__HASH is not string")
		return false
	}
	if *actualSourceHash != sourceHash {
		countlog.Error("event!compiler.SOURCE__HASH mismatch",
			"expected", sourceHash,
			"actual", *actualSourceHash)
		return false
	}
	return true
}
