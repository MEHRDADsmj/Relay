package domain

// DefaultPageLimit is used when PageRequest.Limit is unset (0).
const DefaultPageLimit = 50

// MinPageLimit / MaxPageLimit bound PageRequest.Limit.
const (
	MinPageLimit = 1
	MaxPageLimit = 200
)

// PageRequest is a keyset pagination request.
type PageRequest struct {
	Limit  int
	Cursor string
}

// Page is one page of items with an opaque next cursor (empty = last page).
type Page[T any] struct {
	Items      []T
	NextCursor string
}
