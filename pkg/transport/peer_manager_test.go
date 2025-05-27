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

func (f *MockPeerFactory) CreatePeer(address string) *MockPeer {
	args := f.Called(address)
	return args.Get(0).(*MockPeer)
}

func TestNewPeerManager(t *testing.T) {
	manager := NewPeerManager[*MockPeer, ibabuza.RaftStatusReporter]()

	assert.NotNil(t, manager)
	assert.IsType(t, &PeerManagerImpl[*MockPeer, ibabuza.RaftStatusReporter]{}, manager)

	managerImpl := manager
	assert.NotNil(t, managerImpl.peers)
	assert.NotNil(t, managerImpl.addresses)
	assert.NotNil(t, managerImpl.refCounts)
}

func TestManagerImpl_AddPeer(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager[*MockPeer, ibabuza.RaftStatusReporter]()

	// Setup
	mockPeer := new(MockPeer)
	mockPeer.On("Run").Return()

	// Test adding new peer
	groupID := ibabuza.RaftGroupID(1)
	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerAddress).Return(mockPeer)

	err := manager.AddPeer(groupID, peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Verify the peer was added
	assert.Equal(t, mockPeer, manager.peers[peerAddress])
	identifier := RaftPeerIdentifier{GroupID: groupID, PeerID: peerID}
	assert.Equal(t, peerAddress, manager.addresses[identifier])

	// Test adding duplicate peer
	err = manager.AddPeer(groupID, peerID, peerAddress, factory)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "peer already exists")
}

func TestManagerImpl_GetPeer(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager[*MockPeer, ibabuza.RaftStatusReporter]()

	// Setup
	mockPeer := new(MockPeer)
	mockPeer.On("Run").Return()

	groupID := ibabuza.RaftGroupID(1)
	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerAddress).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(groupID, peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test getting existing peer
	p, err := manager.GetPeer(groupID, peerID)
	assert.NoError(t, err)
	assert.Equal(t, mockPeer, p)

	// Test getting non-existent peer
	_, err = manager.GetPeer(groupID, 999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "peer not found")
}

func TestManagerImpl_UpdatePeer(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager[*MockPeer, ibabuza.RaftStatusReporter]()

	// Setup
	mockPeer := new(MockPeer)
	groupID := ibabuza.RaftGroupID(1)
	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerAddress).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(groupID, peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test updating with same address (no restart)
	err = manager.UpdatePeer(groupID, peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Setup for address change
	newAddress := "localhost:10002"
	newMockPeer := new(MockPeer)
	factory.On("CreatePeer", newAddress).Return(newMockPeer)

	// Test updating with new address
	err = manager.UpdatePeer(groupID, peerID, newAddress, factory)
	assert.NoError(t, err)

	identifier := RaftPeerIdentifier{GroupID: groupID, PeerID: peerID}
	assert.Equal(t, newAddress, manager.addresses[identifier])

	// Test updating non-existent peer
	err = manager.UpdatePeer(groupID, 999, peerAddress, factory)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "peer not found")
}

func TestManagerImpl_RemovePeer(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager[*MockPeer, ibabuza.RaftStatusReporter]()

	// Setup
	mockPeer := new(MockPeer)
	groupID := ibabuza.RaftGroupID(1)
	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerAddress).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(groupID, peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test removing peer
	err = manager.RemovePeer(groupID, peerID)
	assert.NoError(t, err)

	// Verify peer removed
	identifier := RaftPeerIdentifier{GroupID: groupID, PeerID: peerID}
	_, addrExists := manager.addresses[identifier]
	assert.False(t, addrExists)
	_, peerExists := manager.peers[peerAddress]
	assert.False(t, peerExists)

	// Test removing non-existent peer
	err = manager.RemovePeer(groupID, peerID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "peer not found")
}

func TestManagerImpl_RemoveAllPeers(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager[*MockPeer, ibabuza.RaftStatusReporter]()

	// Setup multiple peers
	groupID := ibabuza.RaftGroupID(1)
	peerIDs := []uint64{1, 2, 3}

	for i, id := range peerIDs {
		mockPeer := new(MockPeer)
		peerAddress := "localhost:1000" + string('0'+rune(i))
		factory.On("CreatePeer", peerAddress).Return(mockPeer)

		err := manager.AddPeer(groupID, id, peerAddress, factory)
		assert.NoError(t, err)
	}

	// Test removing all peers
	manager.RemoveAllPeers()

	// Verify all peers removed
	assert.Empty(t, manager.peers)
	assert.Empty(t, manager.addresses)
	assert.Empty(t, manager.refCounts)
}

func TestManagerImpl_GetPeerAddress(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager[*MockPeer, ibabuza.RaftStatusReporter]()

	// Setup
	mockPeer := new(MockPeer)
	groupID := ibabuza.RaftGroupID(1)
	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerAddress).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(groupID, peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test getting address of existing peer
	address, err := manager.ResolvePeerAddress(groupID, peerID)
	assert.NoError(t, err)
	assert.Equal(t, peerAddress, address)

	// Test getting address of non-existent peer
	_, err = manager.ResolvePeerAddress(groupID, 999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "peer address not found")
}

func TestManagerImpl_GetPeerByAddress(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager[*MockPeer, ibabuza.RaftStatusReporter]()

	// Setup
	mockPeer := new(MockPeer)
	groupID := ibabuza.RaftGroupID(1)
	peerID := uint64(1)
	peerAddress := "localhost:10001"
	factory.On("CreatePeer", peerAddress).Return(mockPeer)

	// Add peer
	err := manager.AddPeer(groupID, peerID, peerAddress, factory)
	assert.NoError(t, err)

	// Test getting peer by address
	peer, err := manager.GetPeerByAddress(peerAddress)
	assert.NoError(t, err)
	assert.Equal(t, mockPeer, peer)

	// Test getting non-existent address
	_, err = manager.GetPeerByAddress("nonexistent:1234")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "peer not found for address")
}

func TestManagerImpl_UpdatePeerRaftReport(t *testing.T) {
	factory := new(MockPeerFactory)
	manager := NewPeerManager[*MockPeer, ibabuza.RaftStatusReporter]()

	// Setup multiple peers
	groupID := ibabuza.RaftGroupID(1)
	peerIDs := []uint64{1, 2, 3}
	mockPeers := make([]*MockPeer, len(peerIDs))

	for i, id := range peerIDs {
		mockPeer := new(MockPeer)
		mockPeer.On("UpdateRaftReport", mock.Anything).Return()
		mockPeers[i] = mockPeer

		peerAddress := "localhost:1000" + string('0'+rune(i))
		factory.On("CreatePeer", peerAddress).Return(mockPeer)

		err := manager.AddPeer(groupID, id, peerAddress, factory)
		assert.NoError(t, err)
	}

	// Create a mock RaftStatusReporter
	mockReport := struct{ ibabuza.RaftStatusReporter }{}

	// Test updating all peers with report
	manager.UpdatePeerRaftReport(mockReport)

	// Verify all peers received the report
	for _, p := range mockPeers {
		p.AssertCalled(t, "UpdateRaftReport", mockReport)
	}
}
