package types

type WasmFile struct {
	Name    string `json:"name"`
	Version string `json:"Version"`
}

// This type only stores JSON, so no need to implement custom marshalling
