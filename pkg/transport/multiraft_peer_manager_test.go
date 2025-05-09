package transport

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/ibabuza/babuzapb"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

// MockMultiRaftPeer implements the MultiRaftPeer interface for testing
type MockMultiRaftPeer struct {
	mock.Mock
	id uint64
}

func (m *MockMultiRaftPeer) SendRaftMessage(msg *babuzapb.MultiRaftMessage) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockMultiRaftPeer) SendSnapshot(msg babuzapb.MultiRaftMessage, snapReader peer.SnapshotFileReader) {
	m.Called(msg, snapReader)
}

func (m *MockMultiRaftPeer) UpdateRaftReport(report ibabuza.MultiRaftStatusReporter) {
	m.Called(report)
}

func (m *MockMultiRaftPeer) Stop() {
	m.Called()
}

// MockMultiRaftPeerFactory implements MultiRaftPeerFactory interface for testing
type MockMultiRaftPeerFactory struct {
	mock.Mock
}

func (f *MockMultiRaftPeerFactory) CreatePeer(peerID uint64) peer.MultiRaftPeer {
	args := f.Called(peerID)
	return args.Get(0).(peer.MultiRaftPeer)
}

func TestNewMultiRaftPeerManager(t *testing.T) {
	manager := NewMultiRaftPeerManager()

	assert.NotNil(t, manager)
	assert.IsType(t, &MultiRaftManagerImpl{}, manager)
}

func TestMultiRaftManagerImpl_GetPeer(t *testing.T) {
	factory := new(MockMultiRaftPeerFactory)
	manager := NewMultiRaftPeerManager()

	// Setup
	mockPeer := new(MockMultiRaftPeer)

	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerID).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test getting existing peer
	p, _ := manager.GetPeer(peerID)
	assert.Equal(t, mockPeer, p)

	// Test getting non-existent peer
	_, err = manager.GetPeer(999)
	assert.Error(t, err)
}

func TestMultiRaftManagerImpl_AddPeer(t *testing.T) {
	factory := new(MockMultiRaftPeerFactory)
	manager := NewMultiRaftPeerManager()

	// Setup
	mockPeer := new(MockMultiRaftPeer)

	// Test adding new peer
	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerID).Return(mockPeer)

	err := manager.AddPeer(peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Verify the peer was added
	m, ok := manager.peers.Load(peerID)
	assert.True(t, ok)
	assert.Equal(t, mockPeer, m.(*MockMultiRaftPeer))
	a, ok := manager.addresses.Load(peerID)
	assert.True(t, ok)
	assert.Equal(t, peerAddress, a.(string))
	factory.AssertCalled(t, "CreatePeer", peerID)

	// Test adding duplicate peer
	err = manager.AddPeer(peerID, peerAddress, factory)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestMultiRaftManagerImpl_UpdatePeer(t *testing.T) {
	factory := new(MockMultiRaftPeerFactory)
	manager := NewMultiRaftPeerManager()

	// Setup
	mockPeer := new(MockMultiRaftPeer)

	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerID).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test updating address
	newAddress := "localhost:10002"
	err = manager.UpdatePeer(peerID, newAddress)
	assert.NoError(t, err)
	a, ok := manager.addresses.Load(peerID)
	assert.True(t, ok)
	assert.Equal(t, newAddress, a.(string))

	// Test updating non-existent peer
	err = manager.UpdatePeer(999, peerAddress)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestMultiRaftManagerImpl_RemovePeer(t *testing.T) {
	factory := new(MockMultiRaftPeerFactory)
	manager := NewMultiRaftPeerManager()

	// Setup
	mockPeer := new(MockMultiRaftPeer)
	mockPeer.On("Stop").Return()

	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerID).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test removing peer
	err = manager.RemovePeer(peerID)
	assert.NoError(t, err)

	// Verify peer stopped and removed
	mockPeer.AssertCalled(t, "Stop")
	_, ok := manager.peers.Load(peerID)
	assert.False(t, ok)
	_, ok = manager.addresses.Load(peerID)
	assert.False(t, ok)

	// Test removing non-existent peer
	err = manager.RemovePeer(peerID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestMultiRaftManagerImpl_RemoveAllPeers(t *testing.T) {
	factory := new(MockMultiRaftPeerFactory)
	manager := NewMultiRaftPeerManager()

	// Setup multiple peers
	peerIDs := []uint64{1, 2, 3}
	mockPeers := make([]*MockMultiRaftPeer, len(peerIDs))

	for i, id := range peerIDs {
		mockPeer := new(MockMultiRaftPeer)
		mockPeer.On("Stop").Return()
		mockPeers[i] = mockPeer

		peerAddress := "localhost:1000" + string('0'+rune(i))
		factory.On("CreatePeer", id).Return(mockPeer)

		err := manager.AddPeer(id, peerAddress, factory)
		assert.NoError(t, err)
	}

	// Test removing all peers
	manager.RemoveAllPeers()

	// Verify all peers stopped and removed
	for i, p := range mockPeers {
		p.AssertCalled(t, "Stop")
		id := peerIDs[i]
		_, ok := manager.peers.Load(id)
		assert.False(t, ok)
		_, ok = manager.addresses.Load(id)
		assert.False(t, ok)
	}

}

func TestMultiRaftManagerImpl_ResolvePeerAddress(t *testing.T) {
	factory := new(MockMultiRaftPeerFactory)
	manager := NewMultiRaftPeerManager()

	// Setup
	mockPeer := new(MockMultiRaftPeer)

	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerID).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test getting address of existing peer
	address, err := manager.ResolvePeerAddress(peerID)
	assert.NoError(t, err)
	assert.Equal(t, peerAddress, address)

	// Test getting address of non-existent peer
	_, err = manager.ResolvePeerAddress(999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestMultiRaftManagerImpl_UpdatePeerRaftReport(t *testing.T) {
	factory := new(MockMultiRaftPeerFactory)
	manager := NewMultiRaftPeerManager()

	// Setup multiple peers
	peerIDs := []uint64{1, 2, 3}
	mockPeers := make([]*MockMultiRaftPeer, len(peerIDs))

	for i, id := range peerIDs {
		mockPeer := new(MockMultiRaftPeer)
		mockPeers[i] = mockPeer

		peerAddress := "localhost:1000" + string('0'+rune(i))
		factory.On("CreatePeer", id).Return(mockPeer)

		err := manager.AddPeer(id, peerAddress, factory)
		assert.NoError(t, err)
	}

	// Create a mock RaftStatusReporter
	mockReport := new(struct {
		ibabuza.MultiRaftStatusReporter
	})

	// Set expectations for all peers
	for _, p := range mockPeers {
		p.On("UpdateRaftReport", mock.Anything).Return()
	}

	// Test updating all peers with report
	manager.UpdatePeerRaftReport(*mockReport)

	// Verify all peers received the report
	for _, p := range mockPeers {
		p.AssertCalled(t, "UpdateRaftReport", *mockReport)
	}
}
