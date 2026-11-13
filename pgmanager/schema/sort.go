package schema

import (
	"slices"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// sortColumns are the sorts of a list, and the expressions of each, which
// are sorted in turn, in the same direction
type sortColumns map[string][]string

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// orderBy returns the ORDER BY clause of a list, by the expressions of the
// sort, in the order, asc or desc, with empty values last in either, and
// then by the ties, ascending, so the pages are stable. Without a sort, it's
// the list's own order, unsorted, whatever the order. A sort, or order,
// which isn't one is a bad parameter.
func orderBy(sort, order string, columns sortColumns, unsorted string, ties ...string) (string, error) {
	if sort == "" {
		return unsorted, nil
	}
	exprs, exists := columns[strings.ToLower(sort)]
	if !exists {
		return "", pg.ErrBadParameter.Withf("invalid sort parameter %q", sort)
	}
	var direction string
	switch strings.ToLower(order) {
	case "", "asc":
		direction = "ASC"
	case "desc":
		direction = "DESC"
	default:
		return "", pg.ErrBadParameter.Withf("invalid order parameter %q", order)
	}
	terms := make([]string, 0, len(exprs)+len(ties))
	for _, expr := range exprs {
		terms = append(terms, expr+" "+direction+" NULLS LAST")
	}
	for _, tie := range ties {
		if !slices.Contains(exprs, tie) {
			terms = append(terms, tie+" ASC")
		}
	}
	return "ORDER BY " + strings.Join(terms, ", "), nil
}
