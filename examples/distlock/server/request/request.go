package request

type LockAcquireRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	LockName                          string `json:"lock_name"`
	OwnerID                           string `json:"owner_id"`
	LeaseID                           uint64 `json:"lease_id"`
	WaitTimeoutSeconds                int64  `json:"wait_timeout_seconds,omitempty"`
	RequestID                         string `json:"request_id,omitempty"`
}

type LockReleaseRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	LockName                          string `json:"lock_name"`
	OwnerID                           string `json:"owner_id"`
	FencingToken                      uint64 `json:"fencing_token"`
}

type LeaseGrantRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	TTLSeconds                        int64  `json:"ttl_seconds"`
}

type LeaseRevokeRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	LeaseID                           uint64 `json:"lease_id"`
}

type LeaseKeepAliveRequest struct {
	SessionID                         uint64 `json:"session_id"`
	SequenceNumber                    uint64 `json:"sequence_number"`
	LowestSequenceNumberNotYetReplied uint64 `json:"lowest_sequence_number_not_yet_replied"`
	LeaseID                           uint64 `json:"lease_id"`
}
