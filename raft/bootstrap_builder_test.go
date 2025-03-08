package raft

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBootstrapBuilder_Build_RequiredFields(t *testing.T) {
	builder := NewBootstrapBuilder()

	// Test without any configuration
	_, err := builder.Build()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config is required")

	// Test with config but without peersConfig
	builder.SetConfig(&BabuzaConfig{})
	_, err = builder.Build()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "peersConfig is required")
}

func TestBootstrapBuilder_NonExistentFolder(t *testing.T) {
	builder := NewBootstrapBuilder()
	builder.SetConfig(&BabuzaConfig{})
	builder.SetPeersConfig(&VotingPeersConfiguration{})

	// Test with non-existent directory
	nonExistentDir := "/non/existent/dir"
	builder.SetDefaultStorageDir(nonExistentDir)
	_, err := builder.Build()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "defaultStorageDir does not exist")
}
