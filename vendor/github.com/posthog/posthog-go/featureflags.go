package posthog

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const LONG_SCALE = 0xfffffffffffffff

type FeatureFlagsPoller struct {
	ticker                       *time.Ticker // periodic ticker
	loaded                       chan bool
	shutdown                     chan bool
	forceReload                  chan bool
	featureFlags                 []FeatureFlag
	groups                       map[string]string
	personalApiKey               string
	projectApiKey                string
	Errorf                       func(format string, args ...interface{})
	Endpoint                     string
	http                         http.Client
	mutex                        sync.RWMutex
	fetchedFlagsSuccessfullyOnce bool
}

type FeatureFlag struct {
	Key                        string `json:"key"`
	IsSimpleFlag               bool   `json:"is_simple_flag"`
	RolloutPercentage          *uint8 `json:"rollout_percentage"`
	Active                     bool   `json:"active"`
	Filters                    Filter `json:"filters"`
	EnsureExperienceContinuity *bool  `json:"ensure_experience_continuity"`
}

type Filter struct {
	AggregationGroupTypeIndex *uint8          `json:"aggregation_group_type_index"`
	Groups                    []PropertyGroup `json:"groups"`
	Multivariate              *Variants       `json:"multivariate"`
}

type Variants struct {
	Variants []FlagVariant `json:"variants"`
}

type FlagVariant struct {
	Key               string `json:"key"`
	Name              string `json:"name"`
	RolloutPercentage *uint8 `json:"rollout_percentage"`
}
type PropertyGroup struct {
	Properties        []Property `json:"properties"`
	RolloutPercentage *uint8     `json:"rollout_percentage"`
}

type Property struct {
	Key      string      `json:"key"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
	Type     string      `json:"type"`
}

type FlagVariantMeta struct {
	ValueMin float64
	ValueMax float64
	Key      string
}

type FeatureFlagsResponse struct {
	Flags            []FeatureFlag      `json:"flags"`
	GroupTypeMapping *map[string]string `json:"group_type_mapping"`
}

type DecideRequestData struct {
	ApiKey           string                `json:"api_key"`
	DistinctId       string                `json:"distinct_id"`
	Groups           Groups                `json:"groups"`
	PersonProperties Properties            `json:"person_properties"`
	GroupProperties  map[string]Properties `json:"group_properties"`
}

type DecideResponse struct {
	FeatureFlags map[string]interface{} `json:"featureFlags"`
}

type InconclusiveMatchError struct {
	msg string
}

func (e *InconclusiveMatchError) Error() string {
	return e.msg
}

func newFeatureFlagsPoller(projectApiKey string, personalApiKey string, errorf func(format string, args ...interface{}), endpoint string, httpClient http.Client, pollingInterval time.Duration) *FeatureFlagsPoller {
	poller := FeatureFlagsPoller{
		ticker:                       time.NewTicker(pollingInterval),
		loaded:                       make(chan bool),
		shutdown:                     make(chan bool),
		forceReload:                  make(chan bool),
		personalApiKey:               personalApiKey,
		projectApiKey:                projectApiKey,
		Errorf:                       errorf,
		Endpoint:                     endpoint,
		http:                         httpClient,
		mutex:                        sync.RWMutex{},
		fetchedFlagsSuccessfullyOnce: false,
	}

	go poller.run()
	return &poller
}

func (poller *FeatureFlagsPoller) run() {
	poller.fetchNewFeatureFlags()

	for {
		select {
		case <-poller.shutdown:
			close(poller.shutdown)
			close(poller.forceReload)
			close(poller.loaded)
			poller.ticker.Stop()
			return
		case <-poller.forceReload:
			poller.fetchNewFeatureFlags()
		case <-poller.ticker.C:
			poller.fetchNewFeatureFlags()
		}
	}
}

func (poller *FeatureFlagsPoller) fetchNewFeatureFlags() {
	personalApiKey := poller.personalApiKey
	headers := [][2]string{{"Authorization", "Bearer " + personalApiKey + ""}}
	res, err := poller.localEvaluationFlags(headers)
	if err != nil || res.StatusCode != http.StatusOK {
		poller.loaded <- false
		poller.Errorf("Unable to fetch feature flags", err)
	}
	defer res.Body.Close()
	resBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		poller.loaded <- false
		poller.Errorf("Unable to fetch feature flags", err)
		return
	}
	featureFlagsResponse := FeatureFlagsResponse{}
	err = json.Unmarshal([]byte(resBody), &featureFlagsResponse)
	if err != nil {
		poller.loaded <- false
		poller.Errorf("Unable to unmarshal response from api/feature_flag/local_evaluation", err)
		return
	}
	if !poller.fetchedFlagsSuccessfullyOnce {
		poller.loaded <- true
	}
	newFlags := []FeatureFlag{}
	for _, flag := range featureFlagsResponse.Flags {
		newFlags = append(newFlags, flag)
	}
	poller.mutex.Lock()
	poller.featureFlags = newFlags
	if featureFlagsResponse.GroupTypeMapping != nil {
		poller.groups = *featureFlagsResponse.GroupTypeMapping
	}
	poller.fetchedFlagsSuccessfullyOnce = true
	poller.mutex.Unlock()

}

func (poller *FeatureFlagsPoller) GetFeatureFlag(flagConfig FeatureFlagPayload) (interface{}, error) {

	featureFlags := poller.GetFeatureFlags()

	featureFlag := FeatureFlag{Key: ""}

	// avoid using flag for conflicts with Golang's stdlib `flag`
	for _, storedFlag := range featureFlags {
		if flagConfig.Key == storedFlag.Key {
			featureFlag = storedFlag
			break
		}
	}

	var result interface{}
	var err error

	if featureFlag.Key != "" {
		result, err = poller.computeFlagLocally(featureFlag, flagConfig.DistinctId, flagConfig.Groups, flagConfig.PersonProperties, flagConfig.GroupProperties)
	}

	if err != nil {
		poller.Errorf("Unable to compute flag locally - %s", err)
	}

	if (err != nil || result == nil) && !flagConfig.OnlyEvaluateLocally {

		result, err = poller.getFeatureFlagVariant(featureFlag, flagConfig.Key, flagConfig.DistinctId, flagConfig.PersonProperties, flagConfig.GroupProperties)
		if err != nil {
			return nil, nil
		}
	}

	return result, err
}

func (poller *FeatureFlagsPoller) GetAllFlags(flagConfig FeatureFlagPayloadNoKey) (map[string]interface{}, error) {
	response := map[string]interface{}{}
	featureFlags := poller.GetFeatureFlags()
	fallbackToDecide := false

	if len(featureFlags) == 0 {
		fallbackToDecide = true
	} else {
		for _, storedFlag := range featureFlags {
			result, err := poller.computeFlagLocally(storedFlag, flagConfig.DistinctId, flagConfig.Groups, flagConfig.PersonProperties, flagConfig.GroupProperties)
			if err != nil {
				poller.Errorf("Unable to compute flag locally - %s", err)
				fallbackToDecide = true
			} else {
				response[storedFlag.Key] = result
			}
		}
	}

	if fallbackToDecide && !flagConfig.OnlyEvaluateLocally {
		result, err := poller.getFeatureFlagVariants(flagConfig.DistinctId, flagConfig.Groups, flagConfig.PersonProperties, flagConfig.GroupProperties)

		if err != nil {
			return response, err
		} else {
			for k, v := range result {
				response[k] = v
			}
		}
	}

	return response, nil
}

func (poller *FeatureFlagsPoller) computeFlagLocally(flag FeatureFlag, distinctId string, groups Groups, personProperties Properties, groupProperties map[string]Properties) (interface{}, error) {
	if flag.EnsureExperienceContinuity != nil && *flag.EnsureExperienceContinuity {
		return nil, &InconclusiveMatchError{"Flag has experience continuity enabled"}
	}

	if !flag.Active {
		return false, nil
	}

	if flag.Filters.AggregationGroupTypeIndex != nil {

		groupName, exists := poller.groups[fmt.Sprintf("%d", *flag.Filters.AggregationGroupTypeIndex)]

		if !exists {
			errMessage := "Flag has unknown group type index"
			return nil, errors.New(errMessage)
		}

		_, exists = groups[groupName]

		if !exists {
			errMessage := fmt.Sprintf("FEATURE FLAGS] Can't compute group feature flag: %s without group names passed in", flag.Key)
			return nil, errors.New(errMessage)
		}

		focusedGroupProperties := groupProperties[groupName]
		return matchFeatureFlagProperties(flag, groups[groupName].(string), focusedGroupProperties)
	} else {
		return matchFeatureFlagProperties(flag, distinctId, personProperties)
	}
}

func getMatchingVariant(flag FeatureFlag, distinctId string) (interface{}, error) {
	lookupTable := getVariantLookupTable(flag)

	hashValue, err := _hash(flag.Key, distinctId, "variant")

	if err != nil {
		return nil, err
	}

	for _, variant := range lookupTable {
		if hashValue >= float64(variant.ValueMin) && hashValue < float64(variant.ValueMax) {
			return variant.Key, nil
		}
	}

	return true, nil
}

func getVariantLookupTable(flag FeatureFlag) []FlagVariantMeta {
	lookupTable := []FlagVariantMeta{}
	valueMin := 0.00

	multivariates := flag.Filters.Multivariate

	if multivariates == nil || multivariates.Variants == nil {
		return lookupTable
	}

	for _, variant := range multivariates.Variants {
		valueMax := float64(valueMin) + float64(*variant.RolloutPercentage)/100
		_flagVariantMeta := FlagVariantMeta{ValueMin: float64(valueMin), ValueMax: valueMax, Key: variant.Key}
		lookupTable = append(lookupTable, _flagVariantMeta)
		valueMin = float64(valueMax)
	}

	return lookupTable

}

func matchFeatureFlagProperties(flag FeatureFlag, distinctId string, properties Properties) (interface{}, error) {
	conditions := flag.Filters.Groups
	isInconclusive := false

	for _, condition := range conditions {
		isMatch, err := isConditionMatch(flag, distinctId, condition, properties)

		if err != nil {
			if _, ok := err.(*InconclusiveMatchError); ok {
				isInconclusive = true
			} else {
				return nil, err
			}
		}

		if isMatch {
			return getMatchingVariant(flag, distinctId)
		}
	}

	if isInconclusive {
		return false, &InconclusiveMatchError{"Can't determine if feature flag is enabled or not with given properties"}
	}

	return false, nil
}

func isConditionMatch(flag FeatureFlag, distinctId string, condition PropertyGroup, properties Properties) (bool, error) {

	if len(condition.Properties) > 0 {
		for _, prop := range condition.Properties {

			isMatch, err := matchProperty(prop, properties)
			if err != nil {
				return false, err
			}

			if !isMatch {
				return false, nil
			}
		}

		if condition.RolloutPercentage != nil {
			return true, nil
		}
	}

	if condition.RolloutPercentage != nil {
		return checkIfSimpleFlagEnabled(flag.Key, distinctId, *condition.RolloutPercentage)
	}

	return true, nil
}

func matchProperty(property Property, properties Properties) (bool, error) {
	key := property.Key
	operator := property.Operator
	value := property.Value
	if _, ok := properties[key]; !ok {
		return false, &InconclusiveMatchError{"Can't match properties without a given property value"}
	}

	if operator == "is_not_set" {
		return false, &InconclusiveMatchError{"Can't match properties with operator is_not_set"}
	}

	override_value, _ := properties[key]

	if operator == "exact" {
		switch t := value.(type) {
		case []interface{}:
			return contains(t, override_value), nil
		default:
			return value == override_value, nil
		}
	}

	if operator == "is_not" {
		switch t := value.(type) {
		case []interface{}:
			return !contains(t, override_value), nil
		default:
			return value != override_value, nil
		}
	}

	if operator == "is_set" {
		return true, nil
	}

	if operator == "icontains" {
		return strings.Contains(strings.ToLower(fmt.Sprintf("%v", override_value)), strings.ToLower(fmt.Sprintf("%v", value))), nil
	}

	if operator == "not_icontains" {
		return !strings.Contains(strings.ToLower(fmt.Sprintf("%v", override_value)), strings.ToLower(fmt.Sprintf("%v", value))), nil
	}

	if operator == "regex" {

		r, err := regexp.Compile(fmt.Sprintf("%v", value))

		// invalid regex
		if err != nil {
			return false, nil
		}

		match := r.MatchString(fmt.Sprintf("%v", override_value))

		if match {
			return true, nil
		} else {
			return false, nil
		}
	}

	if operator == "not_regex" {
		var r *regexp.Regexp
		var err error

		if valueString, ok := value.(string); ok {
			r, err = regexp.Compile(valueString)
		} else if valueInt, ok := value.(int); ok {
			valueString = strconv.Itoa(valueInt)
			r, err = regexp.Compile(valueString)
		} else {
			errMessage := "Regex expression not allowed"
			return false, errors.New(errMessage)
		}

		// invalid regex
		if err != nil {
			return false, nil
		}

		var match bool
		if valueString, ok := override_value.(string); ok {
			match = r.MatchString(valueString)
		} else if valueInt, ok := override_value.(int); ok {
			valueString = strconv.Itoa(valueInt)
			match = r.MatchString(valueString)
		} else {
			errMessage := "Value type not supported"
			return false, errors.New(errMessage)
		}

		if !match {
			return true, nil
		} else {
			return false, nil
		}
	}

	if operator == "gt" {
		valueOrderable, overrideValueOrderable, err := validateOrderable(value, override_value)
		if err != nil {
			return false, err
		}

		return overrideValueOrderable > valueOrderable, nil
	}

	if operator == "lt" {
		valueOrderable, overrideValueOrderable, err := validateOrderable(value, override_value)
		if err != nil {
			return false, err
		}

		return overrideValueOrderable < valueOrderable, nil
	}

	if operator == "gte" {
		valueOrderable, overrideValueOrderable, err := validateOrderable(value, override_value)
		if err != nil {
			return false, err
		}

		return overrideValueOrderable >= valueOrderable, nil
	}

	if operator == "lte" {
		valueOrderable, overrideValueOrderable, err := validateOrderable(value, override_value)
		if err != nil {
			return false, err
		}

		return overrideValueOrderable <= valueOrderable, nil
	}

	return false, nil

}

func validateOrderable(firstValue interface{}, secondValue interface{}) (float64, float64, error) {
	convertedFirstValue, err := interfaceToFloat(firstValue)

	if err != nil {
		errMessage := "Value 1 is not orderable"
		return 0, 0, errors.New(errMessage)
	}
	convertedSecondValue, err := interfaceToFloat(secondValue)

	if err != nil {
		errMessage := "Value 2 is not orderable"
		return 0, 0, errors.New(errMessage)
	}

	return convertedFirstValue, convertedSecondValue, nil

}

func interfaceToFloat(val interface{}) (float64, error) {

	var i float64
	switch t := val.(type) {
	case int:
		i = float64(t)
	case int8:
		i = float64(t)
	case int16:
		i = float64(t)
	case int32:
		i = float64(t)
	case int64:
		i = float64(t)
	case float32:
		i = float64(t)
	case float64:
		i = float64(t)
	case uint8:
		i = float64(t)
	case uint16:
		i = float64(t)
	case uint32:
		i = float64(t)
	case uint64:
		i = float64(t)
	default:
		errMessage := "Argument not orderable"
		return 0.0, errors.New(errMessage)
	}

	return i, nil
}

func contains(s []interface{}, e interface{}) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (poller *FeatureFlagsPoller) isSimpleFlagEnabled(key string, distinctId string, rolloutPercentage uint8) (bool, error) {
	isEnabled, err := checkIfSimpleFlagEnabled(key, distinctId, rolloutPercentage)
	if err != nil {
		errMessage := "Error converting string to int"
		poller.Errorf(errMessage)
		return false, errors.New(errMessage)
	}
	return isEnabled, nil
}

// extracted as a regular func for testing purposes
func checkIfSimpleFlagEnabled(key string, distinctId string, rolloutPercentage uint8) (bool, error) {
	val, err := _hash(key, distinctId, "")

	if err != nil {
		return false, err
	}

	return val <= float64(rolloutPercentage)/100, nil
}

func _hash(key string, distinctId string, salt string) (float64, error) {
	hash := sha1.New()
	hash.Write([]byte("" + key + "." + distinctId + "" + salt))
	digest := hash.Sum(nil)
	hexString := fmt.Sprintf("%x\n", digest)[:15]

	value, err := strconv.ParseInt(hexString, 16, 64)
	if err != nil {
		return 0, err
	}

	return float64(value) / LONG_SCALE, nil

}

func (poller *FeatureFlagsPoller) GetFeatureFlags() []FeatureFlag {
	// ensure flags are loaded on the first call

	if !poller.fetchedFlagsSuccessfullyOnce {
		<-poller.loaded
	}

	return poller.featureFlags
}

func (poller *FeatureFlagsPoller) decide(requestData []byte, headers [][2]string) (*http.Response, error) {
	localEvaluationEndpoint := "decide/?v=2"

	url, err := url.Parse(poller.Endpoint + "/" + localEvaluationEndpoint + "")

	if err != nil {
		poller.Errorf("creating url - %s", err)
	}

	return poller.request("POST", url, requestData, headers)
}

func (poller *FeatureFlagsPoller) localEvaluationFlags(headers [][2]string) (*http.Response, error) {
	localEvaluationEndpoint := "api/feature_flag/local_evaluation"

	url, err := url.Parse(poller.Endpoint + "/" + localEvaluationEndpoint + "")

	if err != nil {
		poller.Errorf("creating url - %s", err)
	}
	searchParams := url.Query()
	searchParams.Add("token", poller.projectApiKey)
	url.RawQuery = searchParams.Encode()

	return poller.request("GET", url, []byte{}, headers)
}

func (poller *FeatureFlagsPoller) request(method string, url *url.URL, requestData []byte, headers [][2]string) (*http.Response, error) {

	req, err := http.NewRequest(method, url.String(), bytes.NewReader(requestData))
	if err != nil {
		poller.Errorf("creating request - %s", err)
	}

	version := getVersion()

	req.Header.Add("User-Agent", "posthog-go (version: "+version+")")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", fmt.Sprintf("%d", len(requestData)))

	for _, header := range headers {
		req.Header.Add(header[0], header[1])
	}

	res, err := poller.http.Do(req)

	if err != nil {
		poller.Errorf("sending request - %s", err)
	}

	return res, err
}

func (poller *FeatureFlagsPoller) ForceReload() {
	poller.forceReload <- true
}

func (poller *FeatureFlagsPoller) shutdownPoller() {
	poller.shutdown <- true
}

func (poller *FeatureFlagsPoller) getFeatureFlagVariants(distinctId string, groups Groups, personProperties Properties, groupProperties map[string]Properties) (map[string]interface{}, error) {
	errorMessage := "Failed when getting flag variants"
	requestDataBytes, err := json.Marshal(DecideRequestData{
		ApiKey:           poller.projectApiKey,
		DistinctId:       distinctId,
		Groups:           groups,
		PersonProperties: personProperties,
		GroupProperties:  groupProperties,
	})
	headers := [][2]string{{"Authorization", "Bearer " + poller.personalApiKey + ""}}
	if err != nil {
		errorMessage = "unable to marshal decide endpoint request data"
		poller.Errorf(errorMessage)
		return nil, errors.New(errorMessage)
	}
	res, err := poller.decide(requestDataBytes, headers)
	if err != nil || res.StatusCode != http.StatusOK {
		errorMessage = "Error calling /decide/"
		poller.Errorf(errorMessage)
		return nil, errors.New(errorMessage)
	}
	resBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		errorMessage = "Error reading response from /decide/"
		poller.Errorf(errorMessage)
		return nil, errors.New(errorMessage)
	}
	defer res.Body.Close()
	decideResponse := DecideResponse{}
	err = json.Unmarshal([]byte(resBody), &decideResponse)
	if err != nil {
		errorMessage = "Error parsing response from /decide/"
		poller.Errorf(errorMessage)
		return nil, errors.New(errorMessage)
	}

	return decideResponse.FeatureFlags, nil
}

func (poller *FeatureFlagsPoller) getFeatureFlagVariant(featureFlag FeatureFlag, key string, distinctId string, personProperties Properties, groupProperties map[string]Properties) (interface{}, error) {
	var result interface{} = false

	if featureFlag.IsSimpleFlag {

		// json.Unmarshal will convert JSON `null` to a nullish value for each type
		// which is 0 for uint. However, our feature flags should have rolloutPercentage == 100
		// if it is set to `null`. Having rollout percentage be a pointer and deferencing it
		// here allows its value to be `nil` following json.Unmarhsal, so we can appropriately
		// set it to 100
		rolloutPercentage := uint8(100)
		if featureFlag.RolloutPercentage != nil {
			rolloutPercentage = *featureFlag.RolloutPercentage
		}
		var err error
		result, err = poller.isSimpleFlagEnabled(key, distinctId, rolloutPercentage)
		if err != nil {
			return false, err
		}
	} else {
		featureFlagVariants, variantErr := poller.getFeatureFlagVariants(distinctId, nil, personProperties, groupProperties)

		if variantErr != nil {
			return false, variantErr
		}

		for flagKey, flagValue := range featureFlagVariants {
			var flagValueString = fmt.Sprintf("%v", flagValue)
			if key == flagKey && flagValueString != "false" {
				result = flagValueString
				break
			}
		}
		return result, nil
	}
	return result, nil
}
