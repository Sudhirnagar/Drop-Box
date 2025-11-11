// Storage for application state
let nodes = [];
let files = {};

// Initialize the application
document.addEventListener('DOMContentLoaded', function () {
    initializeDropZone();
    loadConfiguration();
    log('System initialized', 'info');
});

// ============================================================================
// DROP ZONE FUNCTIONALITY
// ============================================================================

function initializeDropZone() {
    const dropZone = document.getElementById('dropZone');
    const fileInput = document.getElementById('fileInput');

    // Click to browse
    dropZone.addEventListener('click', () => {
        fileInput.click();
    });

    // File selection via input
    fileInput.addEventListener('change', (e) => {
        handleFiles(e.target.files);
    });

    // Drag and drop events
    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.add('drag-over');
    });

    dropZone.addEventListener('dragleave', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.remove('drag-over');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.remove('drag-over');

        const droppedFiles = e.dataTransfer.files;
        handleFiles(droppedFiles);
    });
}

// ============================================================================
// FILE HANDLING
// ============================================================================

async function handleFiles(fileList) {
    if (fileList.length === 0) return;

    if (nodes.length === 0) {
        alert('Please add at least one storage node before uploading files.');
        log('Upload failed: No nodes configured', 'error');
        return;
    }

    log(`Uploading ${fileList.length} file(s)...`, 'info');

    for (let file of fileList) {
        await uploadFile(file);
    }
}

async function uploadFile(file) {
    const replicationFactor = parseInt(document.getElementById('replicationFactor').value);
    const chunkSize = parseInt(document.getElementById('chunkSize').value);

    log(`Starting upload: ${file.name} (${formatBytes(file.size)})`, 'info');
    showProgress(true);

    try {
        // Read file
        const fileData = await readFileAsArrayBuffer(file);

        // Partition into chunks
        const chunks = partitionFile(fileData, chunkSize);
        log(`File split into ${chunks.length} chunks`, 'info');

        // Store file metadata
        files[file.name] = {
            name: file.name,
            size: file.size,
            chunks: chunks.length,
            timestamp: new Date().toISOString(),
            chunkMapping: {}
        };

        // Upload each chunk with replication
        for (let i = 0; i < chunks.length; i++) {
            updateProgress((i / chunks.length) * 100, `Uploading chunk ${i + 1}/${chunks.length}`);

            const chunk = chunks[i];
            const numReplicas = Math.min(replicationFactor, nodes.length);
            const targetNodes = [];

            // Select nodes for replication (round-robin)
            for (let j = 0; j < numReplicas; j++) {
                const nodeIndex = (i + j) % nodes.length;
                targetNodes.push(nodes[nodeIndex].id);
            }

            files[file.name].chunkMapping[i] = targetNodes;
            log(`Chunk ${i} replicated to nodes: ${targetNodes.join(', ')}`, 'info');
        }

        updateProgress(100, 'Upload complete!');
        log(`✓ Upload complete: ${file.name}`, 'success');

        setTimeout(() => {
            showProgress(false);
            refreshFilesList();
        }, 1500);

    } catch (error) {
        log(`✗ Upload failed: ${error.message}`, 'error');
        showProgress(false);
    }
}

function readFileAsArrayBuffer(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = (e) => resolve(e.target.result);
        reader.onerror = (e) => reject(new Error('Failed to read file'));
        reader.readAsArrayBuffer(file);
    });
}

function partitionFile(arrayBuffer, chunkSize) {
    const chunks = [];
    const view = new Uint8Array(arrayBuffer);

    for (let i = 0; i < view.length; i += chunkSize) {
        const chunk = view.slice(i, i + chunkSize);
        const hash = simpleHash(chunk);
        chunks.push({
            id: chunks.length,
            data: chunk,
            hash: hash
        });
    }

    return chunks;
}

function simpleHash(data) {
    let hash = 0;
    for (let i = 0; i < data.length; i++) {
        hash = ((hash << 5) - hash) + data[i];
        hash = hash & hash;
    }
    return hash.toString(16);
}

// ============================================================================
// DOWNLOAD FUNCTIONALITY
// ============================================================================

function downloadFile(filename) {
    if (!files[filename]) {
        alert('File not found!');
        return;
    }

    log(`Downloading: ${filename}`, 'info');

    // Simulate download (in real implementation, this would retrieve chunks from nodes)
    setTimeout(() => {
        log(`✓ Download complete: ${filename}`, 'success');
        alert(`File "${filename}" downloaded successfully!\n\nIn a real implementation, this would:\n1. Retrieve chunks from storage nodes\n2. Verify chunk integrity\n3. Reassemble the file\n4. Save to disk`);
    }, 1000);
}

function deleteFile(filename) {
    if (!files[filename]) {
        alert('File not found!');
        return;
    }

    if (!confirm(`Are you sure you want to delete "${filename}"?`)) {
        return;
    }

    log(`Deleting: ${filename}`, 'info');

    // Delete from storage
    delete files[filename];

    log(`✓ File deleted: ${filename}`, 'success');
    refreshFilesList();
}

// ============================================================================
// NODE MANAGEMENT
// ============================================================================

function addNode() {
    const nodeId = parseInt(document.getElementById('nodeId').value);
    const nodeHost = document.getElementById('nodeHost').value.trim();
    const nodePort = parseInt(document.getElementById('nodePort').value);

    if (!nodeId || !nodeHost || !nodePort) {
        alert('Please fill in all node fields');
        return;
    }

    // Check if node already exists
    if (nodes.find(n => n.id === nodeId)) {
        alert(`Node with ID ${nodeId} already exists`);
        return;
    }

    const node = {
        id: nodeId,
        host: nodeHost,
        port: nodePort,
        status: 'online'
    };

    nodes.push(node);
    log(`Added node ${nodeId} at ${nodeHost}:${nodePort}`, 'success');

    // Clear inputs
    document.getElementById('nodeId').value = '';
    document.getElementById('nodeHost').value = '';
    document.getElementById('nodePort').value = '';

    renderNodes();
    saveConfiguration();
}

function removeNode(nodeId) {
    if (!confirm(`Remove node ${nodeId}?`)) {
        return;
    }

    nodes = nodes.filter(n => n.id !== nodeId);
    log(`Removed node ${nodeId}`, 'info');

    renderNodes();
    saveConfiguration();
}

function renderNodes() {
    const nodesList = document.getElementById('nodesList');

    if (nodes.length === 0) {
        nodesList.innerHTML = `
  <div class="empty-state notice">
    ⚠️ No nodes configured. Add nodes to get started.<br>
    <span>To configure nodes, please check the <b>config.json</b> file.</span></div>`;
        return;
    }

    nodesList.innerHTML = nodes.map(node => `
        <div class="node-item">
            <div class="node-info">
                <span class="node-badge">Node ${node.id}</span>
                <span>${node.host}:${node.port}</span>
            </div>
            <div class="node-actions">
                <span class="node-status ${node.status}">${node.status}</span>
                <button class="btn btn-danger btn-small" onclick="removeNode(${node.id})">Remove</button>
            </div>
        </div>
    `).join('');
}

// ============================================================================
// FILES LIST
// ============================================================================

function refreshFilesList() {
    const filesList = document.getElementById('filesList');
    const fileNames = Object.keys(files);

    if (fileNames.length === 0) {
        filesList.innerHTML = '<div class="empty-state">No files uploaded yet.</div>';
        return;
    }

    filesList.innerHTML = fileNames.map(filename => {
        const file = files[filename];
        return `
            <div class="file-item">
                <div class="file-info">
                    <div class="file-icon">📄</div>
                    <div class="file-details">
                        <h4>${file.name}</h4>
                        <p>${formatBytes(file.size)} • ${file.chunks} chunks • ${new Date(file.timestamp).toLocaleString()}</p>
                    </div>
                </div>
                <div class="file-actions">
                    <button class="btn btn-success btn-small" onclick="downloadFile('${filename}')">Download</button>
                    <button class="btn btn-danger btn-small" onclick="deleteFile('${filename}')">Delete</button>
                </div>
            </div>
        `;
    }).join('');
}

// ============================================================================
// PROGRESS BAR
// ============================================================================

function showProgress(show) {
    const progressDiv = document.getElementById('uploadProgress');
    if (show) {
        progressDiv.classList.remove('hidden');
    } else {
        progressDiv.classList.add('hidden');
        updateProgress(0, '');
    }
}

function updateProgress(percent, text) {
    const progressFill = document.getElementById('progressFill');
    const progressText = document.getElementById('progressText');

    progressFill.style.width = percent + '%';
    progressFill.textContent = Math.round(percent) + '%';
    progressText.textContent = text;
}

// ============================================================================
// LOGGING
// ============================================================================

function log(message, type = 'info') {
    const logsDiv = document.getElementById('logs');
    const timestamp = new Date().toLocaleTimeString();
    const logEntry = document.createElement('div');
    logEntry.className = `log-entry ${type}`;
    logEntry.textContent = `[${timestamp}] ${message}`;

    logsDiv.insertBefore(logEntry, logsDiv.firstChild);

    // Keep only last 50 logs
    while (logsDiv.children.length > 50) {
        logsDiv.removeChild(logsDiv.lastChild);
    }
}

function clearLogs() {
    document.getElementById('logs').innerHTML = '';
    log('Logs cleared', 'info');
}

// ============================================================================
// CONFIGURATION PERSISTENCE
// ============================================================================

function saveConfiguration() {
    const config = {
        replicationFactor: document.getElementById('replicationFactor').value,
        chunkSize: document.getElementById('chunkSize').value,
        nodes: nodes,
        files: files
    };
    localStorage.setItem('distributedStorageConfig', JSON.stringify(config));
}

function loadConfiguration() {
    const saved = localStorage.getItem('distributedStorageConfig');
    if (saved) {
        try {
            const config = JSON.parse(saved);
            document.getElementById('replicationFactor').value = config.replicationFactor || 2;
            document.getElementById('chunkSize').value = config.chunkSize || 1048576;
            nodes = config.nodes || [];
            files = config.files || {};

            renderNodes();
            refreshFilesList();
            log('Configuration loaded', 'success');
        } catch (e) {
            log('Failed to load configuration', 'error');
        }
    }
}

// Save configuration on changes
document.getElementById('replicationFactor').addEventListener('change', saveConfiguration);
document.getElementById('chunkSize').addEventListener('change', saveConfiguration);

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}