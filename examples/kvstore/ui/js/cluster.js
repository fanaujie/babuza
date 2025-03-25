/**
 * Cluster Management
 * Handles cluster operations and status
 */

document.addEventListener('DOMContentLoaded', function() {
    // Get references to UI elements
    const leaderInfo = document.getElementById('leaderInfo');
    const clusterTable = document.getElementById('clusterTable');
    const addPeerForm = document.getElementById('addPeerForm');
    const updatePeerForm = document.getElementById('updatePeerForm');
    
    // Modal action buttons
    const addPeerSubmit = document.getElementById('addPeerSubmit');
    const updatePeerSubmit = document.getElementById('updatePeerSubmit');
    const transferLeaderSubmit = document.getElementById('transferLeaderSubmit');
    
    // Modal instances
    const addPeerModal = new bootstrap.Modal(document.getElementById('addPeerModal'));
    const updatePeerModal = new bootstrap.Modal(document.getElementById('updatePeerModal'));
    const transferLeaderModal = new bootstrap.Modal(document.getElementById('transferLeaderModal'));
    
    // Dropdown selectors
    const transfereePeerID = document.getElementById('transfereePeerID');
    
    // Client instance
    const client = window.babuzaClient;
    
    // Refresh cluster status periodically
    window.refreshClusterStatus = async function() {
        if (!client.connected) return;
        
        try {
            const clusterConfig = await client.getClusterPeers();
            renderClusterStatus(clusterConfig);
            updateTransfereeDropdown(clusterConfig);
            
            // Schedule next refresh
            setTimeout(refreshClusterStatus, 5000);
        } catch (error) {
            console.error('Failed to refresh cluster status:', error);
            leaderInfo.innerHTML = `Failed to load cluster information: ${error.message}`;
            leaderInfo.className = 'alert alert-danger';
            
            const tbody = clusterTable.querySelector('tbody');
            tbody.innerHTML = `
                <tr>
                    <td colspan="5" class="text-center text-danger">Error loading peer information</td>
                </tr>
            `;
        }
    };
    
    // Render the cluster status as a table
    function renderClusterStatus(clusterConfig) {
        if (!clusterConfig || !clusterConfig.peers) {
            leaderInfo.innerHTML = 'No cluster information available';
            leaderInfo.className = 'alert alert-warning';
            return;
        }
        
        const leaderId = clusterConfig.leader_id;
        const peers = clusterConfig.peers;
        
        // Update leader information
        leaderInfo.innerHTML = `<strong>Leader:</strong> Peer ID ${leaderId}`;
        leaderInfo.className = 'alert alert-info';
        
        // Update table body
        const tbody = clusterTable.querySelector('tbody');
        tbody.innerHTML = '';
        
        // Render each peer as a table row
        peers.forEach(peer => {
            const isLeader = peer.id === leaderId;
            const row = document.createElement('tr');
            
            // Apply styling for leader or learner
            if (isLeader) {
                row.classList.add('table-warning');
            } else if (peer.is_learner) {
                row.classList.add('table-info');
            }
            
            // Peer ID
            const peerIdCell = document.createElement('td');
            peerIdCell.textContent = peer.id;
            row.appendChild(peerIdCell);
            
            // Role
            const roleCell = document.createElement('td');
            roleCell.textContent = isLeader ? 'Leader' : (peer.is_learner ? 'Learner' : 'Voter');
            row.appendChild(roleCell);
            
            // Raft Address
            const raftAddressCell = document.createElement('td');
            raftAddressCell.textContent = peer.raft_listen_addr || 'N/A';
            row.appendChild(raftAddressCell);
            
            // Service Address
            const serviceAddressCell = document.createElement('td');
            serviceAddressCell.textContent = peer.app_service_address || 'N/A';
            row.appendChild(serviceAddressCell);
            
            // Action buttons
            const actionsCell = document.createElement('td');
            actionsCell.className = 'text-center';
            
            // Create a button group for actions
            const btnGroup = document.createElement('div');
            btnGroup.className = 'btn-group btn-group-sm';
            
            // Update button - always available
            const updateBtn = document.createElement('button');
            updateBtn.type = 'button';
            updateBtn.className = 'btn btn-primary update-peer';
            updateBtn.dataset.peerId = peer.id;
            updateBtn.dataset.raftAddr = peer.raft_listen_addr || '';
            updateBtn.innerHTML = '<i class="bi bi-pencil"></i> Update';
            updateBtn.addEventListener('click', handleUpdatePeer);
            btnGroup.appendChild(updateBtn);
            
            // Transfer leadership button - only for voters that aren't the leader
            if (!isLeader && !peer.is_learner) {
                const transferBtn = document.createElement('button');
                transferBtn.type = 'button';
                transferBtn.className = 'btn btn-warning transfer-leader';
                transferBtn.dataset.peerId = peer.id;
                transferBtn.innerHTML = '<i class="bi bi-shuffle"></i> Make Leader';
                transferBtn.addEventListener('click', handleTransferLeader);
                btnGroup.appendChild(transferBtn);
            }
            
            // Promote button - only for learners
            if (peer.is_learner) {
                const promoteBtn = document.createElement('button');
                promoteBtn.type = 'button';
                promoteBtn.className = 'btn btn-success promote-learner';
                promoteBtn.dataset.peerId = peer.id;
                promoteBtn.innerHTML = '<i class="bi bi-arrow-up-circle"></i> Promote';
                promoteBtn.addEventListener('click', handlePromoteLearner);
                btnGroup.appendChild(promoteBtn);
            }
            
            // Remove button - available for all peers
            const removeBtn = document.createElement('button');
            removeBtn.type = 'button';
            removeBtn.className = 'btn btn-danger remove-peer';
            removeBtn.dataset.peerId = peer.id;
            removeBtn.innerHTML = '<i class="bi bi-trash"></i> Remove';
            removeBtn.addEventListener('click', handleRemovePeer);
            btnGroup.appendChild(removeBtn);
            
            actionsCell.appendChild(btnGroup);
            row.appendChild(actionsCell);
            
            tbody.appendChild(row);
        });
    }
    
    // Update transferee dropdown for leadership transfer modal
    function updateTransfereeDropdown(clusterConfig) {
        if (!clusterConfig || !clusterConfig.peers) return;
        
        const leaderId = clusterConfig.leader_id;
        const peers = clusterConfig.peers;
        
        // Clear existing options
        transfereePeerID.innerHTML = '<option value="">Select a peer</option>';
        
        // Add peer options
        peers.forEach(peer => {
            // Only add non-leader peers as transferee options
            if (peer.id !== leaderId && !peer.is_learner) {
                const transfereeOption = document.createElement('option');
                transfereeOption.value = peer.id;
                transfereeOption.textContent = `Peer ${peer.id} (${peer.raft_listen_addr})`;
                transfereePeerID.appendChild(transfereeOption);
            }
        });
    }
    
    // Add Peer button handler
    addPeerSubmit.addEventListener('click', async function() {
        const peerId = parseInt(document.getElementById('peerID').value);
        const peerAddr = document.getElementById('peerAddr').value;
        const peerServiceAddr = document.getElementById('peerServiceAddr').value;
        const isLearner = document.getElementById('isLearner').checked;
        
        try {
            await client.addPeer(peerId, peerAddr, isLearner);
            showNotification('Success', `Added peer ${peerId} successfully`);
            refreshClusterStatus();
            addPeerForm.reset();
            addPeerModal.hide();
        } catch (error) {
            showNotification('Error', `Failed to add peer: ${error.message}`, 'error');
        }
    });
    
    // Transfer Leadership button handler
    transferLeaderSubmit.addEventListener('click', async function() {
        const transfereeId = parseInt(transfereePeerID.value);
        if (!transfereeId) {
            showNotification('Error', 'Please select a peer to transfer leadership to', 'error');
            return;
        }
        
        try {
            await client.transferLeader(transfereeId);
            showNotification('Success', `Leadership transfer initiated to peer ${transfereeId}`);
            transferLeaderModal.hide();
            setTimeout(refreshClusterStatus, 1000);
        } catch (error) {
            showNotification('Error', `Failed to transfer leadership: ${error.message}`, 'error');
        }
    });
    
    // Update Peer button handler
    updatePeerSubmit.addEventListener('click', async function() {
        const peerId = parseInt(document.getElementById('updatePeerID').value);
        const raftAddr = document.getElementById('updatePeerRaftAddr').value;
        
        try {
            await client.updatePeer(peerId, raftAddr);
            showNotification('Success', `Updated peer ${peerId} successfully`);
            refreshClusterStatus();
            updatePeerModal.hide();
        } catch (error) {
            showNotification('Error', `Failed to update peer: ${error.message}`, 'error');
        }
    });
    
    // Handlers for row action buttons
    function handleUpdatePeer(e) {
        const btn = e.currentTarget;
        const peerId = parseInt(btn.dataset.peerId);
        const raftAddr = btn.dataset.raftAddr;
        
        // Set form values
        document.getElementById('updatePeerID').value = peerId;
        document.getElementById('updatePeerRaftAddr').value = raftAddr;
        
        // Show modal
        updatePeerModal.show();
    }
    
    async function handleTransferLeader(e) {
        const btn = e.currentTarget;
        const peerId = parseInt(btn.dataset.peerId);
        
        // Set dropdown value
        transfereePeerID.value = peerId;
        
        // Show modal
        transferLeaderModal.show();
    }
    
    async function handlePromoteLearner(e) {
        const btn = e.currentTarget;
        const peerId = parseInt(btn.dataset.peerId);
        
        if (confirm(`Are you sure you want to promote learner ${peerId} to a voting member?`)) {
            try {
                await client.promoteLearner(peerId);
                showNotification('Success', `Promoted learner ${peerId} to voting member`);
                refreshClusterStatus();
            } catch (error) {
                showNotification('Error', `Failed to promote learner: ${error.message}`, 'error');
            }
        }
    }
    
    async function handleRemovePeer(e) {
        const btn = e.currentTarget;
        const peerId = parseInt(btn.dataset.peerId);
        
        if (confirm(`Are you sure you want to remove peer ${peerId} from the cluster?`)) {
            try {
                await client.removePeer(peerId);
                showNotification('Success', `Removed peer ${peerId} from the cluster`);
                refreshClusterStatus();
            } catch (error) {
                showNotification('Error', `Failed to remove peer: ${error.message}`, 'error');
            }
        }
    }
});
