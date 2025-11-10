package metadata

import "strconv"

type Placeholder int

// Next increases the identifier value for the next parameter in the YDB query.
func (p *Placeholder) Next() string {
	*p++
	return "$f" + strconv.Itoa(int(*p))
}

// Named returns a static named parameter with the given name (e.g., $json, $id).
func (p *Placeholder) Named(name string) string {
	return "$f_" + name
}
