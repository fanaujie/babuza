/**
 * Babuza KV-Store Client
 * Handles session management and request tracking
 */

// Global client object
window.babuzaClient = {
    // Client state
    baseUrl: '', // Will be set dynamically
    sessionId: null,
    sequenceNumber: 0,
    pendingRequests: {}, // Map of sequence numbers to pending request statuses
    connected: false,

    // Initialize the client with the base API URL
    init: function(baseUrl) {
        if (baseUrl) {
            // Remove trailing slash if present
            this.baseUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
        } else {
            this.baseUrl = window.location.origin;
        }
        this.sessionId = null;
        this.sequenceNumber = 0;
        this.pendingRequests = {};
        this.connected = false;
        return this;
    },

    // Show loading spinner
    showLoading: function() {
        const spinner = document.getElementById('loadingSpinner');
        if (spinner) {
            spinner.classList.remove('d-none');
        }
    },
    
    // Hide loading spinner
    hideLoading: function() {
        const spinner = document.getElementById('loadingSpinner');
        if (spinner) {
            spinner.classList.add('d-none');
        }
    },

    // Create a new session with the server
    connect: async function() {
        this.showLoading();
        try {
            const response = await fetch(`${this.baseUrl}/sessions`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            if (!response.ok) {
                throw new Error(`Failed to establish session: ${response.status} ${response.statusText}`);
            }

            const data = await response.json();
            this.sessionId = data.session_id;
            this.sequenceNumber = 0;
            this.pendingRequests = {};
            this.connected = true;
            
            console.log(`Connected with session ID: ${this.sessionId}`);
            return this.sessionId;
        } catch (error) {
            console.error('Connection error:', error);
            this.connected = false;
            throw error;
        } finally {
            this.hideLoading();
        }
    },

    // Get the next sequence number for a request
    getNextSequenceNumber: function() {
        return ++this.sequenceNumber;
    },

    // Get the lowest sequence number not yet replied
    getLowestSequenceNumberNotYetReplied: function() {
        const pendingKeys = Object.keys(this.pendingRequests)
            .map(key => parseInt(key))
            .filter(key => this.pendingRequests[key] === 'pending')
            .sort((a, b) => a - b);
        
        return pendingKeys.length > 0 ? pendingKeys[0] : this.sequenceNumber;
    },

    // Make a request to the server with session tracking
    request: async function(method, endpoint, body = null) {
        if (!this.connected || this.sessionId === null) {
            throw new Error('Not connected. Please establish a session first.');
        }

        this.showLoading();
        
        try {
            // Prepare request options
            const options = {
                method: method,
                headers: {
                    'Content-Type': 'application/json'
                }
            };

            // For requests that need client tracking information
            if (method === 'POST' || method === 'PUT' || method === 'DELETE') {
                const seqNum = this.getNextSequenceNumber();
                const lowestSeqNum = this.getLowestSequenceNumberNotYetReplied();
                
                // Track this request as pending
                this.pendingRequests[seqNum] = 'pending';
                
                // Add client tracking information to the body
                body = {
                    ...body,
                    session_id: this.sessionId,
                    sequence_number: seqNum,
                    lowest_sequence_number_not_yet_replied: lowestSeqNum
                };
            }

            // Add body to request if it exists and it's not a GET request
            if (body && method !== 'GET') {
                options.body = JSON.stringify(body);
            }

            // Add query parameters for GET requests with key parameter
            let url = `${this.baseUrl}${endpoint}`;
            if (method === 'GET' && body && body.key) {
                url = `${url}?key=${encodeURIComponent(body.key)}`;
            }

            const response = await fetch(url, options);
            
            if (!response.ok) {
                throw new Error(`Request failed: ${response.status} ${response.statusText}`);
            }

            const data = await response.json();
            
            // Mark request as completed if it was tracked
            if (method === 'POST' || method === 'PUT' || method === 'DELETE') {
                if (data.sequence_number) {
                    this.pendingRequests[data.sequence_number] = 'completed';
                }
                
                // Clean up completed requests that are no longer needed for tracking
                this.cleanupCompletedRequests();
            }
            
            return data;
        } catch (error) {
            // Mark request as failed if it was tracked
            if (method === 'POST' || method === 'PUT' || method === 'DELETE' && body && body.sequence_number) {
                this.pendingRequests[body.sequence_number] = 'failed';
            }
            
            console.error('Request error:', error);
            throw error;
        } finally {
            this.hideLoading();
        }
    },

    // Clean up completed requests that are no longer needed for tracking
    cleanupCompletedRequests: function() {
        const pendingKeys = Object.keys(this.pendingRequests)
            .map(key => parseInt(key))
            .filter(key => this.pendingRequests[key] === 'pending')
            .sort((a, b) => a - b);
        
        if (pendingKeys.length > 0) {
            const lowestPending = pendingKeys[0];
            
            // Remove all completed requests with sequence numbers lower than the lowest pending request
            Object.keys(this.pendingRequests)
                .map(key => parseInt(key))
                .filter(key => key < lowestPending && this.pendingRequests[key] === 'completed')
                .forEach(key => {
                    delete this.pendingRequests[key];
                });
        }
    },

    // Convenience methods for common API endpoints
    
    // Key-Value Store Operations
    getKeyValue: function(key) {
        return this.request('GET', '/kv', { key });
    },
    
    setKeyValue: function(key, value) {
        return this.request('POST', '/kv', { key, value });
    },
    
    appendKeyValue: function(key, value) {
        return this.request('PUT', '/kv', { key, value });
    },
    
    deleteKeyValue: function(key) {
        return this.request('DELETE', '/kv', { key });
    },
    
    // Cluster Management Operations
    getClusterPeers: function() {
        return this.request('GET', '/peers');
    },
    
    addPeer: function(raftPeerId, raftListenAddr, isLearner = false) {
        return this.request('POST', '/peers', {
            raft_peer_id: raftPeerId,
            raft_listen_addr: raftListenAddr,
            is_learner: isLearner
        });
    },
    
    updatePeer: function(raftPeerId, raftListenAddr) {
        return this.request('PUT', '/peers', {
            raft_peer_id: raftPeerId,
            raft_listen_addr: raftListenAddr
        });
    },
    
    removePeer: function(raftPeerId) {
        return this.request('DELETE', '/peers', {
            raft_peer_id: raftPeerId
        });
    },
    
    promoteLearner: function(raftPeerId) {
        return this.request('PUT', '/promote-learner', {
            raft_peer_id: raftPeerId
        });
    },
    
    transferLeader: function(transferee) {
        return this.request('PUT', '/transfer-leader', {
            transferee: transferee
        });
    }
};
