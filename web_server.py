"""
Web Server for Distributed Cloud Storage
This server provides HTTP API endpoints for the web interface
"""

from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import os
import socket
import pickle
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import hashlib

class StorageAPIHandler(SimpleHTTPRequestHandler):
    """HTTP handler for storage API endpoints"""
    
    # Configuration
    nodes = []
    files = {}
    chunk_size = 1024 * 1024  # 1MB
    replication_factor = 2
    
    def do_GET(self):
        """Handle GET requests"""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/':
            # Serve index.html
            self.path = '/index.html'
            return SimpleHTTPRequestHandler.do_GET(self)
        
        elif parsed_path.path == '/api/nodes':
            # Get list of nodes
            self.send_json_response(200, {'nodes': self.nodes})
        
        elif parsed_path.path == '/api/files':
            # Get list of files
            self.send_json_response(200, {'files': self.files})
        
        elif parsed_path.path.startswith('/api/download/'):
            # Download a file
            filename = parsed_path.path.split('/')[-1]
            self.handle_download(filename)
        
        else:
            # Serve static files (CSS, JS, etc.)
            return SimpleHTTPRequestHandler.do_GET(self)
    
    def do_POST(self):
        """Handle POST requests"""
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length)
        
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/api/nodes':
            # Add a node
            data = json.loads(body)
            self.handle_add_node(data)
        
        elif parsed_path.path == '/api/upload':
            # Upload a file
            self.handle_upload(body)
        
        elif parsed_path.path == '/api/config':
            # Update configuration
            data = json.loads(body)
            self.handle_config(data)
        
        else:
            self.send_json_response(404, {'error': 'Endpoint not found'})
    
    def do_DELETE(self):
        """Handle DELETE requests"""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path.startswith('/api/nodes/'):
            # Remove a node
            node_id = int(parsed_path.path.split('/')[-1])
            self.handle_remove_node(node_id)
        
        elif parsed_path.path.startswith('/api/files/'):
            # Delete a file
            filename = parsed_path.path.split('/')[-1]
            self.handle_delete_file(filename)
        
        else:
            self.send_json_response(404, {'error': 'Endpoint not found'})
    
    def handle_add_node(self, data):
        """Add a storage node"""
        node = {
            'id': data['id'],
            'host': data['host'],
            'port': data['port'],
            'status': 'online'
        }
        self.nodes.append(node)
        self.send_json_response(200, {'message': 'Node added', 'node': node})
    
    def handle_remove_node(self, node_id):
        """Remove a storage node"""
        self.nodes = [n for n in self.nodes if n['id'] != node_id]
        self.send_json_response(200, {'message': 'Node removed'})
    
    def handle_upload(self, file_data):
        """Handle file upload"""
        # In a real implementation, this would:
        # 1. Parse multipart form data
        # 2. Partition the file into chunks
        # 3. Distribute chunks to storage nodes
        # 4. Track chunk locations
        
        # For demo purposes, we'll simulate the process
        filename = f"file_{len(self.files)}.bin"
        
        # Simulate file partitioning
        file_size = len(file_data)
        num_chunks = (file_size + self.chunk_size - 1) // self.chunk_size
        
        self.files[filename] = {
            'name': filename,
            'size': file_size,
            'chunks': num_chunks,
            'chunkMapping': {}
        }
        
        # Simulate chunk distribution
        for i in range(num_chunks):
            target_nodes = [
                self.nodes[(i + j) % len(self.nodes)]['id']
                for j in range(min(self.replication_factor, len(self.nodes)))
            ]
            self.files[filename]['chunkMapping'][i] = target_nodes
        
        self.send_json_response(200, {
            'message': 'File uploaded',
            'filename': filename,
            'chunks': num_chunks
        })
    
    def handle_download(self, filename):
        """Handle file download"""
        if filename not in self.files:
            self.send_json_response(404, {'error': 'File not found'})
            return
        
        # In a real implementation, this would:
        # 1. Retrieve chunks from storage nodes
        # 2. Verify chunk integrity
        # 3. Reassemble the file
        # 4. Send to client
        
        self.send_json_response(200, {
            'message': 'Download initiated',
            'file': self.files[filename]
        })
    
    def handle_delete_file(self, filename):
        """Delete a file"""
        if filename not in self.files:
            self.send_json_response(404, {'error': 'File not found'})
            return
        
        # In a real implementation, send delete commands to all nodes
        del self.files[filename]
        
        self.send_json_response(200, {'message': 'File deleted'})
    
    def handle_config(self, data):
        """Update configuration"""
        if 'replication_factor' in data:
            self.replication_factor = data['replication_factor']
        if 'chunk_size' in data:
            self.chunk_size = data['chunk_size']
        
        self.send_json_response(200, {
            'message': 'Configuration updated',
            'replication_factor': self.replication_factor,
            'chunk_size': self.chunk_size
        })
    
    def send_json_response(self, status_code, data):
        """Send JSON response"""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def log_message(self, format, *args):
        """Override to customize logging"""
        print(f"[{self.log_date_time_string()}] {format % args}")


def run_web_server(host='localhost', port=8000):
    """Run the web server"""
    server_address = (host, port)
    httpd = HTTPServer(server_address, StorageAPIHandler)
    
    print("="*60)
    print("DISTRIBUTED CLOUD STORAGE - WEB SERVER")
    print("="*60)
    print(f"Server running at http://{host}:{port}/")
    print("Press Ctrl+C to stop the server")
    print("="*60)
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.shutdown()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Web server for distributed storage')
    parser.add_argument('--host', default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=8000, help='Server port')
    
    args = parser.parse_args()
    run_web_server(args.host, args.port)