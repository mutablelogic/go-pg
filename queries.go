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
// or -- <identifier>.<identifier>, with any number of dots between them. Each
// statement is stored with its key.
type Queries struct {
	keys    []string
	queries map[string]string
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	// A key is identifiers separated by dots, where an identifier is letters,
	// digits and underscores, which doesn't start with a digit, so a comment
	// which ends in a word and a period, such as "-- form.", isn't a key
	reQuerySeparator = regexp.MustCompile(`^--\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\s*$`)
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewQueries parses SQL statements from a reader and returns a Queries collection.
// SQL statements must be separated by comment lines with a key: -- <identifier>
// or -- <identifier>.<identifier>, with any number of dots, where an identifier
// consists of letters, digits and underscores, and doesn't start with a digit.
// A key is only a key outside of a string, a quoted identifier, or a block
// comment, so a line of a string which looks like one, such as "-- note",
// is part of its string. Other comment lines, such as those which describe a
// statement, or one which ends a sentence with a period, such as "-- form.",
// and comments at the end of a line, are removed from the statement, except
// within strings, quoted identifiers, dollar-quoted bodies, such as those of
// functions, and block comments.
// Returns an error if duplicate keys are found or if reading fails.
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
	var sql lexer

	scanner := bufio.NewScanner(r)
	self := &Queries{
		queries: make(map[string]string),
	}

	// save keeps the statement of the key, without any ; at its end
	save := func() {
		if key != "" {
			self.queries[key] = strings.TrimSuffix(strings.TrimSpace(sql.String()), ";")
			self.keys = append(self.keys, key)
		}
		sql = lexer{}
	}

	for scanner.Scan() {
		line := scanner.Text()

		// A line which is a key, outside of a string, a quoted identifier or
		// a block comment, starts a statement
		if matches := reQuerySeparator.FindStringSubmatch(line); matches != nil && !sql.open() {
			save()

			// Get new key, which can't be a duplicate
			key = matches[1]
			if _, exists := self.queries[key]; exists {
				return nil, ErrBadParameter.Withf("duplicate SQL statement key: %q", key)
			}
			continue
		}

		// Accumulate SQL lines for current statement, without comments
		sql.write(line + "\n")
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Save the last statement, and return success
	save()
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Keys returns a slice of all SQL statement identifiers in the order they were parsed.
func (s *Queries) Keys() []string {
	return s.keys
}

// Query retrieves the SQL statement associated with the given key.
// Returns an empty string if the key does not exist.
func (s *Queries) Query(key string) string {
	if sql, ok := s.queries[key]; ok {
		return sql
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// lexer removes the -- comments from SQL, written to it a part at a time,
// such as a line, as it reads it: lines which are only a comment are removed,
// and a comment at the end of a line is removed with the space before it. A
// -- within a string, such as 'a--b', a quoted identifier, such as "a--b",
// a dollar-quoted body, such as a function's, or a block comment, such as
// /* -- */, which nest, isn't a comment, and a dollar-quoted body and a block
// comment are kept as they are.
type lexer struct {
	result  strings.Builder
	line    strings.Builder
	quote   string // the quote which is open: ', E', ", or a dollar tag
	block   int    // the depth of the block comments which are open
	comment bool   // whether the rest of the line is a comment
}

// stripComments removes the -- comments from a statement
func stripComments(sql string) string {
	var l lexer
	l.write(sql)
	return l.String()
}

// open returns true when a string, a quoted identifier, a dollar-quoted
// body, or a block comment is open, so a line isn't the start of a statement
func (l *lexer) open() bool {
	return l.quote != "" || l.block > 0
}

// String returns the SQL, without its comments, and with its last line, which
// isn't ended
func (l *lexer) String() string {
	l.endLine(false)
	return l.result.String()
}

// endLine writes the line, without any space at its end, unless it was only
// a comment, or a string or a block comment is open
func (l *lexer) endLine(eol bool) {
	text := l.line.String()
	if trimmed := strings.TrimRight(text, " \t\r"); trimmed != "" || !l.comment {
		if !l.open() {
			text = trimmed
		}
		l.result.WriteString(text)
		if eol {
			l.result.WriteByte('\n')
		}
	}
	l.line.Reset()
	l.comment = false
}

// write reads the SQL, which continues what was written before it
func (l *lexer) write(sql string) {
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case c == '\n':
			l.endLine(true)
		case l.comment:
			// Skip the comment to the end of the line
		case l.block > 0:
			// Block comments nest
			l.line.WriteByte(c)
			if c == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				i++
				l.line.WriteByte(sql[i])
				l.block--
			} else if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
				i++
				l.line.WriteByte(sql[i])
				l.block++
			}
		case l.quote == "'" || l.quote == "E'":
			l.line.WriteByte(c)
			if c == '\\' && l.quote == "E'" && i+1 < len(sql) {
				// A backslash escapes the next character in an E'' string
				i++
				l.line.WriteByte(sql[i])
			} else if c == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					// '' is a quote within the string
					i++
					l.line.WriteByte(sql[i])
				} else {
					l.quote = ""
				}
			}
		case l.quote == `"`:
			l.line.WriteByte(c)
			if c == '"' {
				l.quote = ""
			}
		case l.quote != "":
			// A dollar-quoted body ends with its tag
			if strings.HasPrefix(sql[i:], l.quote) {
				l.line.WriteString(l.quote)
				i += len(l.quote) - 1
				l.quote = ""
			} else {
				l.line.WriteByte(c)
			}
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			l.comment = true
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			l.block = 1
			i++
			l.line.WriteString("/*")
		case c == '\'':
			if n := l.line.Len(); n > 0 && (l.line.String()[n-1] == 'E' || l.line.String()[n-1] == 'e') && (n == 1 || !isIdentChar(l.line.String()[n-2])) {
				l.quote = "E'"
			} else {
				l.quote = "'"
			}
			l.line.WriteByte(c)
		case c == '"':
			l.quote = `"`
			l.line.WriteByte(c)
		case c == '$':
			if tag := dollarTag(sql[i:]); tag != "" && (l.line.Len() == 0 || !isIdentChar(l.line.String()[l.line.Len()-1])) {
				l.quote = tag
				l.line.WriteString(tag)
				i += len(tag) - 1
			} else {
				l.line.WriteByte(c)
			}
		default:
			l.line.WriteByte(c)
		}
	}
}

// dollarTag returns the dollar quote at the start of the string, such as $$
// or $body$, or an empty string if it doesn't start with one, such as $1 or
// ${"schema"}
func dollarTag(s string) string {
	for i := 1; i < len(s); i++ {
		switch c := s[i]; {
		case c == '$':
			return s[:i+1]
		case c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 1 && c >= '0' && c <= '9') || c >= 0x80:
			// Part of the tag
		default:
			return ""
		}
	}
	return ""
}

// isIdentChar reports whether the byte can be part of an identifier
func isIdentChar(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c >= 0x80
}
