package jsonschema

import (
	"fmt"
)

func Merge(old JSONSchema, new JSONSchema) (*JSONSchema, error) {
	merged, err := mergeProperties(old.Properties, new.Properties)
	if err != nil {
		return nil, err
	}

	old.Properties = merged

	return &old, nil
}

func mergeProperties(old, new map[string]*Property) (map[string]*Property, error) {
	for name, newVersion := range new {
		oldVersion, oldExists := old[name]
		if !oldExists {
			// New field has been added
			old[name] = newVersion

			// shirt circuit since everything under this key will be new
			continue
		}

		// Validate type didn't change
		if oldVersion.Type != newVersion.Type {
			// Type change, this can't happen, fail
			return nil, fmt.Errorf("field '%s' type changed from '%s' to '%s'",
				oldVersion.ID, oldVersion.Type, newVersion.Type)
		}

		if oldVersion.Type == "array" {
			newArrayProps, err := mergeArray(oldVersion.Items, newVersion.Items)
			if err != nil {
				return nil, err
			}

			oldVersion.Items.Properties = newArrayProps

		}

		// No need to go further since th new property has no children
		if len(newVersion.Properties) == 0 {
			continue
		}

		// Drill down further
		mergedProps, err := mergeProperties(oldVersion.Properties, newVersion.Properties)
		if err != nil {
			return nil, err
		}

		oldVersion.Properties = mergedProps

	}

	return old, nil
}

func mergeArray(old, new *AnyOf) ([]*Property, error) {

	// New schema has empty array, nothing to do. Return copy of old props
	if len(new.Properties) == 0 {
		return old.Properties, nil
	}

	// Old was an empty array, just accept new properties
	if len(old.Properties) == 0 {
		return new.Properties, nil
	}

	// TODO: will have to handle tuples in the future
	if old.Properties[0].Type != new.Properties[0].Type {
		return nil, fmt.Errorf("field '%s' type changed from '%s' to '%s'",
			old.Properties[0].ID, old.Properties[0].Type, new.Properties[0].Type)
	}

	return old.Properties, nil
}
