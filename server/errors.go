package server

import (
	"errors"
)

var (
	ErrClientNotFound      = errors.New("client not found")
	ErrClientAlreadyExists = errors.New("client already exists")
	ErrInvalidSession      = errors.New("invalid session")
	ErrPortExhausted       = errors.New("no available ports")
)
