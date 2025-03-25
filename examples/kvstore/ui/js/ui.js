/**
 * UI Controller
 * Handles general UI operations and navigation
 */

document.addEventListener('DOMContentLoaded', function() {
    // Initialize the client without connecting
    const client = window.babuzaClient.init();
    
    // Navigation elements
    const navCluster = document.getElementById('nav-cluster');
    const navKeyValue = document.getElementById('nav-keyvalue');
    const clusterSection = document.getElementById('cluster-management');
    const keyValueSection = document.getElementById('key-value-operations');
    
    // Connection status elements
    const connectionStatus = document.getElementById('connectionStatus');
    
    // Connection modal elements
    const connectionModal = new bootstrap.Modal(document.getElementById('connectionModal'), {
        backdrop: 'static',
        keyboard: false
    });
    const serverUrlInput = document.getElementById('serverUrl');
    const connectButton = document.getElementById('connectButton');
    const connectionAlert = document.getElementById('connectionAlert');
    
    // Toast elements
    const statusToast = document.getElementById('statusToast');
    const toastTitle = document.getElementById('toastTitle');
    const toastMessage = document.getElementById('toastMessage');
    const toast = new bootstrap.Toast(statusToast);
    
    // Show connection modal on page load
    connectionModal.show();
    
    // Set focus to server URL input
    serverUrlInput.focus();
    
    // Handle connection form submission
    connectButton.addEventListener('click', handleConnect);
    serverUrlInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            handleConnect();
        }
    });
    
    function handleConnect() {
        const serverUrl = serverUrlInput.value.trim();
        if (!serverUrl) {
            updateConnectionAlert('Please enter a server URL', 'danger');
            return;
        }
        
        // Update UI to show connecting state
        connectButton.disabled = true;
        updateConnectionAlert('Connecting to server...', 'info');
        
        // Initialize client with the provided URL
        window.babuzaClient.init(serverUrl);
        connectToServer();
    }
    
    function updateConnectionAlert(message, type) {
        connectionAlert.className = `alert alert-${type}`;
        connectionAlert.innerHTML = `
            <div class="d-flex align-items-center">
                <span class="me-2"><i class="bi bi-${type === 'danger' ? 'exclamation-triangle' : type === 'success' ? 'check-circle' : 'info-circle'}"></i></span>
                <span>${message}</span>
            </div>
        `;
    }
    
    // Navigation event handlers
    navCluster.addEventListener('click', function(e) {
        e.preventDefault();
        setActiveNav(navCluster);
        showSection(clusterSection);
    });
    
    navKeyValue.addEventListener('click', function(e) {
        e.preventDefault();
        setActiveNav(navKeyValue);
        showSection(keyValueSection);
    });
    
    // Helper functions
    function setActiveNav(navElement) {
        navCluster.classList.remove('active');
        navKeyValue.classList.remove('active');
        navElement.classList.add('active');
    }
    
    function showSection(section) {
        clusterSection.classList.remove('active');
        keyValueSection.classList.remove('active');
        section.classList.add('active');
    }
    
    // Connection management
    async function connectToServer() {
        try {
            await client.connect();
            updateConnectionStatus(true);
            updateConnectionAlert('Connected to the server successfully', 'success');
            
            // Close the modal after a short delay to show success message
            setTimeout(() => {
                connectionModal.hide();
                showNotification('Success', 'Connected to the server successfully');
                
                // Initialize components after connection
                refreshClusterStatus();
            }, 1000);
        } catch (error) {
            updateConnectionStatus(false);
            updateConnectionAlert(`Connection failed: ${error.message}`, 'danger');
            connectButton.disabled = false;
        }
    }
    
    function updateConnectionStatus(isConnected) {
        const statusBadge = connectionStatus.querySelector('.badge');
        const sessionIdDisplay = document.getElementById('sessionIdDisplay');
        
        if (isConnected) {
            statusBadge.className = 'badge bg-success me-2';
            statusBadge.textContent = 'Connected';
            sessionIdDisplay.textContent = client.sessionId;
        } else {
            statusBadge.className = 'badge bg-danger me-2';
            statusBadge.textContent = 'Disconnected';
            sessionIdDisplay.textContent = '-';
        }
    }
    
    // Show notification toast
    window.showNotification = function(title, message, type = 'success') {
        toastTitle.textContent = title;
        toastMessage.textContent = message;
        
        // Set toast color based on type
        statusToast.classList.remove('bg-success', 'bg-danger', 'bg-warning');
        switch (type) {
            case 'error':
                statusToast.classList.add('bg-danger');
                statusToast.classList.add('text-white');
                break;
            case 'warning':
                statusToast.classList.add('bg-warning');
                break;
            default:
                statusToast.classList.add('bg-success');
                statusToast.classList.add('text-white');
        }
        
        toast.show();
    };
});
