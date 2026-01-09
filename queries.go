package pg

import (
	"bufio"
	"io"
	"regexp"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Queries represents a collection of named SQL statements parsed from a reader.
// SQL statements are separated by comment lines in the format: -- <identifier>
// Each statement is stored with its identifier as the key.
type Queries struct {
	keys    []string
	queries map[string]string
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	reQuerySeparator = regexp.MustCompile(`^--\s*([a-zA-Z0-9_.-]+)\s*$`)
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewQueries parses SQL statements from a reader and returns a Queries collection.
// SQL statements must be separated by comment lines matching the pattern: -- <identifier>
// where <identifier> consists of alphanumeric characters, underscores, dots, and hyphens.
// Returns an error if duplicate identifiers are found or if reading fails.
//
// Example input format:
//
//	-- user.select
//	SELECT * FROM users WHERE id = $1;
//
//	-- user.insert
//	INSERT INTO users (name, email) VALUES ($1, $2);
func NewQueries(r io.Reader) (*Queries, error) {
	var key string
	var sql strings.Builder

	scanner := bufio.NewScanner(r)
	self := &Queries{
		queries: make(map[string]string),
	}

	for scanner.Scan() {
		line := scanner.Text()

		// Check if line matches separator pattern
		if matches := reQuerySeparator.FindStringSubmatch(line); matches != nil {
			// Save previous statement if exists
			if key != "" {
				self.queries[key] = strings.TrimSpace(sql.String())
				self.keys = append(self.keys, key)
			}

			// Get new key
			key = matches[1]

			// Check for duplicate key
			if _, exists := self.queries[key]; exists {
				return nil, ErrBadParameter.Withf("duplicate SQL statement key: %q", key)
			}

			sql.Reset()
			continue
		}

		// Accumulate SQL lines for current statement
		sql.WriteString(line)
		sql.WriteString("\n")
	}

	// Save the last statement
	if key != "" {
		self.queries[key] = strings.TrimSpace(sql.String())
		self.keys = append(self.keys, key)
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Return success
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Keys returns a slice of all SQL statement identifiers in the order they were parsed.
func (s *Queries) Keys() []string {
	return s.keys
}

// Get retrieves the SQL statement associated with the given key.
// Returns an empty string if the key does not exist.
func (s *Queries) Get(key string) string {
	if sql, ok := s.queries[key]; ok {
		return sql
	}
	return ""
}
