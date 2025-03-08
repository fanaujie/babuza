package session

import "errors"

var (
	ErrSessionExpired = errors.New("session expired")
)
