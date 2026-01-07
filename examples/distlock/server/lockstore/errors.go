package lockstore

import "errors"

var (
	ErrLockNotFound    = errors.New("lock not found")
	ErrNotLockOwner    = errors.New("not lock owner or invalid fencing token")
	ErrLockHeld        = errors.New("lock is held by another owner")
	ErrUnknownCommand  = errors.New("unknown command")
	ErrInvalidKeyType  = errors.New("invalid key type, must be string")
	ErrInvalidTTL      = errors.New("invalid TTL value")
	ErrLeaseNotFound   = errors.New("lease not found")
)
