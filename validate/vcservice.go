package validate

import "github.com/pkg/errors"

var (
	// VCService

	ErrMissingServerOptions = errors.New("ServerOptions cannot be nil")
	ErrMissingAuthToken     = errors.New("AuthToken cannot be nil")
	ErrMissingEtcd          = errors.New("EtcdService cannot be nil")
	ErrMissingConfig        = errors.New("PersistentConfig cannot be nil")
)
