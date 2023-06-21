package types

type Counter struct {
	Namespace string            `json:"namespace"`
	Subsystem string            `json:"subsystem"`
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels"`
	Value     float64           `json:"value"`
}
