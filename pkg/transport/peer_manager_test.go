package transport

import (
	"github.com/fanaujie/babuza/ibabuza"
	"github.com/fanaujie/babuza/pkg/transport/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"testing"
)

// MockPeer implements the Peer interface for testing
type MockPeer struct {
	mock.Mock
	id uint64
}

func (m *MockPeer) UpdatePeer() {
	//TODO implement me
	panic("implement me")
}

func (m *MockPeer) SendRaftMessage(msg raftpb.Message) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockPeer) SendSnapshot(msg raftpb.Message, snapReader peer.SnapshotFileReader) {
	m.Called(msg, snapReader)
}

func (m *MockPeer) UpdateRaftReport(report ibabuza.RaftStatusReporter) {
	m.Called(report)
}

func (m *MockPeer) Stop() {
	m.Called()
}

func (m *MockPeer) Run() {
	m.Called()
}

// MockPeerFactory implements PeerFactory interface for testing
type MockPeerFactory struct {
	mock.Mock
}

func (f *MockPeerFactory) CreatePeer(peerID uint64) peer.Peer {
	args := f.Called(peerID)
	return args.Get(0).(peer.Peer)
}

func TestNewPeerManager(t *testing.T) {
	manager := NewPeerManager()

	assert.NotNil(t, manager)
	assert.IsType(t, &ManagerImpl{}, manager)

	managerImpl := manager.(*ManagerImpl)
	assert.NotNil(t, managerImpl.peers)
	assert.NotNil(t, managerImpl.addresses)
}

func TestManagerImpl_AddPeer(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager().(*ManagerImpl)

	// Setup
	mockPeer := new(MockPeer)
	mockPeer.On("Run").Return()

	// Test adding new peer
	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerID).Return(mockPeer)

	err := manager.AddPeer(peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Verify the peer was added
	assert.Equal(t, mockPeer, manager.peers[peerID])
	assert.Equal(t, peerAddress, manager.addresses[peerID])
	factory.AssertCalled(t, "CreatePeer", peerID)

	// Test adding duplicate peer
	err = manager.AddPeer(peerID, peerAddress, factory)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestManagerImpl_GetPeer(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager().(*ManagerImpl)

	// Setup
	mockPeer := new(MockPeer)
	mockPeer.On("Run").Return()

	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerID).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test getting existing peer
	p := manager.GetPeer(peerID)
	assert.Equal(t, mockPeer, p)

	// Test getting non-existent peer
	p = manager.GetPeer(999)
	assert.Nil(t, p)
}

func TestManagerImpl_UpdatePeer(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager().(*ManagerImpl)

	// Setup
	mockPeer := new(MockPeer)

	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerID).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test updating with same address (no restart)
	err = manager.UpdatePeer(peerID, peerAddress)
	assert.NoError(t, err)

	// Verify peer not restarted
	mockPeer.AssertNotCalled(t, "Stop")

	// Setup for address change
	newAddress := "localhost:10002"

	// Test updating with new address (should restart)
	err = manager.UpdatePeer(peerID, newAddress)
	assert.NoError(t, err)
	assert.Equal(t, newAddress, manager.addresses[peerID])

	// Test updating non-existent peer
	err = manager.UpdatePeer(999, peerAddress)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestManagerImpl_RemovePeer(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager().(*ManagerImpl)

	// Setup
	mockPeer := new(MockPeer)
	mockPeer.On("Run").Return()
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
	_, peerExists := manager.peers[peerID]
	_, addrExists := manager.addresses[peerID]
	assert.False(t, peerExists)
	assert.False(t, addrExists)

	// Test removing non-existent peer
	err = manager.RemovePeer(peerID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestManagerImpl_RemoveAllPeers(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager().(*ManagerImpl)

	// Setup multiple peers
	peerIDs := []uint64{1, 2, 3}
	mockPeers := make([]*MockPeer, len(peerIDs))

	for i, id := range peerIDs {
		mockPeer := new(MockPeer)
		mockPeer.On("Run").Return()
		mockPeer.On("Stop").Return()
		mockPeers[i] = mockPeer

		peerAddress := "localhost:" + string('0'+rune(i))
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
		_, peerExists := manager.peers[id]
		_, addrExists := manager.addresses[id]
		assert.False(t, peerExists)
		assert.False(t, addrExists)
	}

	assert.Empty(t, manager.peers)
	assert.Empty(t, manager.addresses)
}

func TestManagerImpl_GetPeerAddress(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager().(*ManagerImpl)

	// Setup
	mockPeer := new(MockPeer)
	mockPeer.On("Run").Return()

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

func TestManagerImpl_UpdatePeerRaftReport(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager().(*ManagerImpl)

	// Setup multiple peers
	peerIDs := []uint64{1, 2, 3}
	mockPeers := make([]*MockPeer, len(peerIDs))

	for i, id := range peerIDs {
		mockPeer := new(MockPeer)
		mockPeer.On("Run").Return()
		mockPeers[i] = mockPeer

		peerAddress := "localhost:1000" + string('0'+rune(i))
		factory.On("CreatePeer", id).Return(mockPeer)

		err := manager.AddPeer(id, peerAddress, factory)
		assert.NoError(t, err)
	}

	// Create a mock RaftStatusReporter
	mockReport := new(struct{ ibabuza.RaftStatusReporter })

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
