package types

import "github.com/batchcorp/plumber-schemas/build/go/protos/common"

type RuleSet struct {
	Set *common.RuleSet `json:"set"`
}
