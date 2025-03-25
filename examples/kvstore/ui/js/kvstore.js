/**
 * Key-Value Operations
 * Handles key-value CRUD operations with recently viewed keys table
 */

document.addEventListener('DOMContentLoaded', function() {
    // Get references to UI elements
    const kvTable = document.getElementById('kvTable');
    const keySearchInput = document.getElementById('keySearchInput');
    const getKeyButton = document.getElementById('getKeyButton');
    
    // Modal elements and buttons
    const addKeyValueSubmit = document.getElementById('addKeyValueSubmit');
    const editKeyValueSubmit = document.getElementById('editKeyValueSubmit');
    const appendKeyValueSubmit = document.getElementById('appendKeyValueSubmit');
    
    // Modal instances
    const addKeyValueModal = new bootstrap.Modal(document.getElementById('addKeyValueModal'));
    const editKeyValueModal = new bootstrap.Modal(document.getElementById('editKeyValueModal'));
    const appendKeyValueModal = new bootstrap.Modal(document.getElementById('appendKeyValueModal'));
    
    // Client instance
    const client = window.babuzaClient;
    
    // Store for recently viewed keys (since API has no list functionality)
    const keyValueCache = {
        items: {},            // Map of key to {value, timestamp}
        maxItems: 50,        // Maximum number of keys to remember
        
        // Add or update a key-value in the cache
        set: function(key, value) {
            this.items[key] = {
                value: value,
                timestamp: Date.now()
            };
            
            // Clean up if we exceed maximum size
            this.prune();
        },
        
        // Get a value by key
        get: function(key) {
            return this.items[key] ? this.items[key].value : null;
        },
        
        // Remove a key
        remove: function(key) {
            if (this.items[key]) {
                delete this.items[key];
            }
        },
        
        // Get all cached keys, sorted by most recent
        getAllKeys: function() {
            return Object.keys(this.items)
                .sort((a, b) => this.items[b].timestamp - this.items[a].timestamp);
        },
        
        // Trim the cache to maximum size
        prune: function() {
            const keys = this.getAllKeys();
            if (keys.length > this.maxItems) {
                // Remove oldest keys
                for (let i = this.maxItems; i < keys.length; i++) {
                    delete this.items[keys[i]];
                }
            }
        },
        
        // Clear all cached items
        clear: function() {
            this.items = {};
        }
    };
    
    // Initialize the key-value table
    initKeyValueTable();
    
    // Get Key button click handler
    getKeyButton.addEventListener('click', async function() {
        await fetchKey();
    });
    
    // Key search input enter key handler
    keySearchInput.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            e.preventDefault();
            getKeyButton.click();
        }
    });
    
    // Add Key-Value submit handler
    addKeyValueSubmit.addEventListener('click', async function() {
        const key = document.getElementById('newKey').value.trim();
        const value = document.getElementById('newValue').value;
        
        if (!key) {
            showNotification('Error', 'Please enter a key', 'error');
            return;
        }
        
        try {
            await client.setKeyValue(key, value);
            keyValueCache.set(key, value);
            showNotification('Success', `Added key '${key}' successfully`);
            refreshKeyValueTable();
            document.getElementById('newKey').value = '';
            document.getElementById('newValue').value = '';
            addKeyValueModal.hide();
        } catch (error) {
            showNotification('Error', `Failed to add key: ${error.message}`, 'error');
        }
    });
    
    // Edit Key-Value submit handler
    editKeyValueSubmit.addEventListener('click', async function() {
        const key = document.getElementById('editKey').value.trim();
        const value = document.getElementById('editValue').value;
        
        try {
            await client.setKeyValue(key, value);
            keyValueCache.set(key, value);
            showNotification('Success', `Updated key '${key}' successfully`);
            refreshKeyValueTable();
            editKeyValueModal.hide();
        } catch (error) {
            showNotification('Error', `Failed to update key: ${error.message}`, 'error');
        }
    });
    
    // Append Key-Value submit handler
    appendKeyValueSubmit.addEventListener('click', async function() {
        const key = document.getElementById('appendKey').value.trim();
        const valueToAppend = document.getElementById('appendValue').value;
        
        try {
            await client.appendKeyValue(key, valueToAppend);
            
            // After appending, we need to fetch the updated value
            const result = await client.getKeyValue(key);
            keyValueCache.set(key, result.value);
            
            showNotification('Success', `Appended to key '${key}' successfully`);
            refreshKeyValueTable();
            appendKeyValueModal.hide();
        } catch (error) {
            showNotification('Error', `Failed to append to key: ${error.message}`, 'error');
        }
    });
    
    // Initialize the key-value table
    function initKeyValueTable() {
        refreshKeyValueTable();
    }
    
    // Fetch a key from the server
    async function fetchKey() {
        const key = keySearchInput.value.trim();
        if (!key) {
            showNotification('Error', 'Please enter a key to fetch', 'error');
            return null;
        }
        
        try {
            const result = await client.getKeyValue(key);
            keyValueCache.set(key, result.value || '');
            showNotification('Success', `Retrieved value for key '${key}'`);
            refreshKeyValueTable();
            return result.value || '';
        } catch (error) {
            showNotification('Error', `Failed to get value: ${error.message}`, 'error');
            return null;
        }
    }
    
    // Delete a key
    async function deleteKey(key) {
        if (confirm(`Are you sure you want to delete key '${key}'?`)) {
            try {
                await client.deleteKeyValue(key);
                keyValueCache.remove(key);
                showNotification('Success', `Deleted key '${key}'`);
                refreshKeyValueTable();
            } catch (error) {
                showNotification('Error', `Failed to delete key: ${error.message}`, 'error');
            }
        }
    }
    
    // Handle edit button click
    function handleEditClick(key) {
        const value = keyValueCache.get(key);
        
        document.getElementById('editKey').value = key;
        document.getElementById('editKeyDisplay').value = key;
        document.getElementById('editValue').value = value;
        
        editKeyValueModal.show();
    }
    
    // Handle append button click
    function handleAppendClick(key) {
        document.getElementById('appendKey').value = key;
        document.getElementById('appendKeyDisplay').value = key;
        document.getElementById('appendValue').value = '';
        
        appendKeyValueModal.show();
    }
    
    // Refresh the key-value table with current data
    function refreshKeyValueTable() {
        const cachedKeys = keyValueCache.getAllKeys();
        const tableBody = kvTable.querySelector('tbody');
        tableBody.innerHTML = '';
        
        if (cachedKeys.length === 0) {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td colspan="3" class="text-center">
                    No keys viewed yet. Enter a key in the search box to retrieve it.
                </td>
            `;
            tableBody.appendChild(row);
        } else {
            cachedKeys.forEach(key => {
                const value = keyValueCache.get(key);
                const row = document.createElement('tr');
                
                // Create table cells
                const keyCell = document.createElement('td');
                keyCell.textContent = key;
                row.appendChild(keyCell);
                
                const valueCell = document.createElement('td');
                
                // Truncate long values for display in the table
                const truncateLength = 100;
                const displayValue = value.length > truncateLength
                    ? value.substring(0, truncateLength) + '...'
                    : value;
                
                valueCell.textContent = displayValue;
                row.appendChild(valueCell);
                
                // Create action buttons
                const actionsCell = document.createElement('td');
                actionsCell.className = 'text-center';
                
                const btnGroup = document.createElement('div');
                btnGroup.className = 'btn-group btn-group-sm';
                
                // Edit button
                const editBtn = document.createElement('button');
                editBtn.type = 'button';
                editBtn.className = 'btn btn-primary';
                editBtn.innerHTML = '<i class="bi bi-pencil"></i>';
                editBtn.title = 'Edit';
                editBtn.addEventListener('click', () => handleEditClick(key));
                btnGroup.appendChild(editBtn);
                
                // Append button
                const appendBtn = document.createElement('button');
                appendBtn.type = 'button';
                appendBtn.className = 'btn btn-info';
                appendBtn.innerHTML = '<i class="bi bi-plus-circle"></i>';
                appendBtn.title = 'Append';
                appendBtn.addEventListener('click', () => handleAppendClick(key));
                btnGroup.appendChild(appendBtn);
                
                // Delete button
                const deleteBtn = document.createElement('button');
                deleteBtn.type = 'button';
                deleteBtn.className = 'btn btn-danger';
                deleteBtn.innerHTML = '<i class="bi bi-trash"></i>';
                deleteBtn.title = 'Delete';
                deleteBtn.addEventListener('click', () => deleteKey(key));
                btnGroup.appendChild(deleteBtn);
                
                actionsCell.appendChild(btnGroup);
                row.appendChild(actionsCell);
                
                tableBody.appendChild(row);
            });
        }
    }
});
