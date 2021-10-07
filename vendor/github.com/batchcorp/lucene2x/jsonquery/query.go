package jsonquery

import (
	"strconv"
	"strings"

	"github.com/batchcorp/lucene2x/lexer"
	"github.com/batchcorp/lucene2x/token"

	"github.com/minio/pkg/wildcard"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

type comparison string
type operator string

const (
	equal        comparison = "="
	notEqual     comparison = "!="
	lessEqual    comparison = "<="
	greaterEqual comparison = ">="
	lessThan     comparison = "<"
	greaterThan  comparison = ">"

	AND operator = "AND"
	OR  operator = "OR"
)

// Clause represents a single query clause
// Ex: some.field="abc"
type Clause struct {
	Path       []string
	Comparison comparison
	Query      string
}

// ClauseGroup represents a logically grouped number of clauses
// Ex: some.field: "abc" AND other.field: "def"
type ClauseGroup struct {
	Clauses        []*Clause
	InsideOperator operator
}

// Query represents a query of a JSON payload
type Query struct {
	Raw          string // raw un-parsed query in string format
	Payload      gjson.Result
	ClauseGroups []*ClauseGroup
}

func Find(jsonData []byte, luceneQuery string) (bool, error) {
	// Lex query
	l := lexer.New(luceneQuery)
	tokens := l.ReadAll()

	// Ensure valid query
	if len(tokens) == 0 {
		// TODO: how can I improve this?
		return false, errors.New("could not understand query")
	}

	// Convert tokens to clause groups
	clauseGroups, err := tokensToClauses(tokens)
	if err != nil {
		return false, err
	}

	q := &Query{
		Raw:          luceneQuery,
		ClauseGroups: clauseGroups,
		Payload:      gjson.ParseBytes(jsonData),
	}

	return q.EvaluateGroups()
}

// tokensToClauses takes a list of lexer tokens and creates query clauses and groups them together
func tokensToClauses(tokens []token.Token) ([]*ClauseGroup, error) {

	clauseGroups := make([]*ClauseGroup, 0)

	clause := &Clause{
		Comparison: equal,
	}
	group := &ClauseGroup{
		Clauses: make([]*Clause, 0),
	}

	for _, t := range tokens {
		if t.Type == token.NOT {
			clause.Comparison = notEqual
		}
		if t.Type == token.LESSEQUAL {
			clause.Comparison = lessEqual
		}
		if t.Type == token.GREATEREQUAL {
			clause.Comparison = greaterEqual
		}
		if t.Type == token.LESSTHAN {
			clause.Comparison = lessThan
		}
		if t.Type == token.GREATERTHAN {
			clause.Comparison = greaterThan
		}

		if t.Type == token.AND {
			group.InsideOperator = AND

			// End of this clause, append to group and init fresh clause
			group.Clauses = append(group.Clauses, clause)
			clause = &Clause{
				Comparison: equal,
			}
			continue
		}
		if t.Type == token.OR {
			group.InsideOperator = OR

			// End of this clause, append to group and init fresh clause
			group.Clauses = append(group.Clauses, clause)
			clause = &Clause{
				Comparison: equal,
			}
			continue
		}

		if t.Type == token.STRING {
			clause.Query = t.Literal
			continue
		}

		if t.Type == token.QUERY {
			clause.Path = strings.Split(t.Literal, ".")
			continue
		}

		// Start of a new group
		if t.Type == token.LPAREN {
			// End of this group
			clauseGroups = append(clauseGroups, group)
			group = &ClauseGroup{
				Clauses: make([]*Clause, 0),
			}
			clause = &Clause{
				Comparison: equal,
			}
			continue
		}

		if t.Type == token.EOF {
			// Wrap up group
			group.Clauses = append(group.Clauses, clause)
			clauseGroups = append(clauseGroups, group)
		}
	}

	return clauseGroups, nil
}

func (q *Query) EvaluateGroups() (bool, error) {

GROUP:
	for _, g := range q.ClauseGroups {
		for _, c := range g.Clauses {
			clauseResult, err := q.EvaluateClause(c)
			if err != nil {
				return false, err
			}

			//fmt.Printf("Clause %+v: %t\n", c, clauseResult)

			// Value found
			if clauseResult == true {
				// This clause group only requires 1 positive, so consider it
				// satisfied and short circuit to next clause group
				if g.InsideOperator == OR {
					continue GROUP
				}

				// And query, need to evaluate the remaining clauses in this group
				continue
			}

			// Value not found

			if g.InsideOperator == AND || g.InsideOperator == "" {
				// This is entire query is a fail
				// AND needs all clauses in the group to match
				// If the inside operator is empty, it's a solo query
				return false, nil
			}

			// this is an OR grouped clause, continue checking clauses to see if any more match
		}

		// All clauses good at this point
	}

	return true, nil
}

// EvaluateClause evaluates a single clause. Ex: path.to.key = "abc"
func (q *Query) EvaluateClause(c *Clause) (bool, error) {
	match := strings.ReplaceAll(c.Query, `\"`, `"`)

	v, err := walkJson(q.Payload, c.Path, match, c.Comparison)
	if err != nil {
		return false, err
	}

	return v != nil, nil
}

// match gets passed along so we can check key.[].key's value
func walkArray(result gjson.Result, query []string, match string, comp comparison) (*gjson.Result, error) {
	var v *gjson.Result
	var err error

	// Looking for any element in this array
	if query[0] == "*" || query[0] == "[]" {
		result.ForEach(func(key, value gjson.Result) bool {
			if value.IsArray() {
				// Array inside of an array. Ex: [['a'], ['b']]
				v, err = walkArray(value, query[1:], match, comp)
			} else if value.IsObject() {
				// Objects inside of an array: [{key: "Val"}]
				v, err = walkJson(value, query[1:], match, comp)
				if v != nil {
					// Break out of ForEach()
					return false
				}
				v = nil
			} else {
				matched, matchErr := isMatch(value, match, comp)
				if matchErr != nil {
					err = matchErr
					return false
				}

				if matched {
					// Array of values. Ex: ["Mark", "Tim"]
					v = &value
					return false // exit ForEach()
				}
			}

			return true
		})

		return v, err
	}

	// Looking for a specific element in this array
	if strings.HasPrefix(query[0], "[") && strings.HasSuffix(query[0], "]") {
		var currentIdx int

		r := strings.NewReplacer("[", "", "]", "")
		// Specific element check
		foundIdx := r.Replace(query[0])

		idx, idxErr := strconv.Atoi(foundIdx)
		if err != nil {
			err = idxErr
			return nil, err
		}

		result.ForEach(func(key, value gjson.Result) bool {
			if idx == currentIdx {
				// Found the element we're looking for
				if value.IsArray() {
					// Array inside of an array. Ex: [['a'], ['b']]
					v, err = walkArray(value, query[1:], match, comp)
				} else if value.IsObject() {
					// Objects inside of an array: [{key: "Val"}]
					v, err = walkJson(value, query[1:], match, comp)
				} else if value.String() == match {
					// Array of values. Ex: ["Mark", "Tim"]
					v = &value
				}

				return false
			}
			currentIdx++
			return true
		})
	}

	return v, err
}

// walkJson is a recursive function which talks through a JSON object and attempts to find a field based
// on a comma-delimited path. When found, it will compare the field's value to `match` parameter, and
// return true if they match
func walkJson(result gjson.Result, query []string, match string, comp comparison) (*gjson.Result, error) {
	var v *gjson.Result
	var err error

	result.ForEach(func(key, value gjson.Result) bool {
		// Safety check
		if len(query) == 0 {
			err = errors.New("empty query")
			return false
		}

		if strings.ToLower(key.String()) != strings.ToLower(query[0]) {
			// Go onto next key
			return true
		}

		// We're at the correct level, now find the field
		if len(query) == 1 {
			matched, matchErr := isMatch(value, match, comp)
			if matchErr != nil {
				err = matchErr
				return false
			}

			if matched {
				// This will either be an array or a scalar, so just return, and then we will check
				// possibly array values upstream in the query parser
				v = &value
				return false // exit ForEach()
			}

			return true
		}

		// We're on the right path, keep diving into this key
		if value.IsObject() {
			v, err = walkJson(value, query[1:], match, comp)
		} else if value.IsArray() {
			v, err = walkArray(value, query[1:], match, comp)
		} else {
			v = nil
			err = errors.New("shouldn't get here")
			return false
		}

		return false // exit ForEach()

	})

	return v, err
}

// isMatch determines if a field's value matches the query's comparison type and value
func isMatch(value gjson.Result, match string, comp comparison) (bool, error) {
	switch value.Type {
	case gjson.String:
		return isMatchString(value.String(), match, comp)
	case gjson.Number:
		return isMatchNumber(value.String(), match, comp)
	}

	return false, nil
}

// isMatchString determines if a stirng field's value matches the query's comparison type and value
func isMatchString(value, match string, comp comparison) (bool, error) {
	switch comp {
	case notEqual:
		return value != match, nil
	default:
		if !strings.Contains(match, "*") {
			// Literal match
			return value == match, nil
		}

		// Wildcard match
		return wildcard.MatchSimple(match, value), nil
	}

	return false, nil
}

// isMatchString determines if a numeric field's value matches the query's comparison type and value
func isMatchNumber(value, match string, comp comparison) (bool, error) {
	switch comp {
	case notEqual:
		return value != match, nil
	case equal:
		return value == match, nil
	default:
		// Greater than, less than, less equal, and greater equals

		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return false, errors.Wrapf(err, "unable to parse value '%s'", value)
		}
		m, err := strconv.ParseFloat(match, 64)
		if err != nil {
			return false, errors.Wrapf(err, "unable to parse query '%s'", match)
		}

		switch comp {
		case lessThan:
			return v < m, nil
		case greaterThan:
			return v > m, nil
		case lessEqual:
			return v <= m, nil
		case greaterEqual:
			return v >= m, nil
		}
	}

	return false, nil
}
