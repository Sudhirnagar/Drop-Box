import socket
import threading
import json
import hashlib
import os
import time
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple
import pickle

# ============================================================================
# MODULE 1: FILE PARTITIONING
# ============================================================================

class FilePartitioner:
    """Handles splitting files into chunks and reassembling them"""
    
    def __init__(self, chunk_size=1024 * 1024):  # 1MB chunks
        self.chunk_size = chunk_size
    
    def partition_file(self, filepath: str) -> List[Tuple[int, bytes, str]]:
        """
        Split file into chunks with metadata
        Returns: List of (chunk_id, chunk_data, chunk_hash)
        """
        chunks = []
        with open(filepath, 'rb') as f:
            chunk_id = 0
            while True:
                chunk_data = f.read(self.chunk_size)
                if not chunk_data:
                    break
                
                # Create hash for integrity checking
                chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                chunks.append((chunk_id, chunk_data, chunk_hash))
                chunk_id += 1
        
        return chunks
    
    def reassemble_file(self, chunks: List[Tuple[int, bytes]], output_path: str):
        """Reassemble chunks back into original file"""
        # Sort chunks by ID
        sorted_chunks = sorted(chunks, key=lambda x: x[0])
        
        with open(output_path, 'wb') as f:
            for _, chunk_data in sorted_chunks:
                f.write(chunk_data)
    
    def verify_chunk(self, chunk_data: bytes, expected_hash: str) -> bool:
        """Verify chunk integrity using hash"""
        actual_hash = hashlib.sha256(chunk_data).hexdigest()
        return actual_hash == expected_hash


# ============================================================================
# MODULE 2: STORAGE NODES
# ============================================================================

class StorageNode:
    """Individual storage node that holds file chunks"""
    
    def __init__(self, node_id: int, host: str, port: int, storage_dir: str):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # Metadata: {filename: {chunk_id: (chunk_hash, storage_path)}}
        self.metadata: Dict[str, Dict[int, Tuple[str, str]]] = {}
        self.running = False
        self.socket = None
    
    def start(self):
        """Start the storage node server"""
        self.running = True
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        
        print(f"[Node {self.node_id}] Started on {self.host}:{self.port}")
        
        while self.running:
            try:
                self.socket.settimeout(1.0)
                client_socket, address = self.socket.accept()
                thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,)
                )
                thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"[Node {self.node_id}] Error: {e}")
    
    def handle_client(self, client_socket: socket.socket):
        """Handle client requests"""
        try:
            # Receive request
            data = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                if len(chunk) < 4096:
                    break
            
            request = pickle.loads(data)
            command = request.get('command')
            
            if command == 'STORE':
                response = self.store_chunk(
                    request['filename'],
                    request['chunk_id'],
                    request['chunk_data'],
                    request['chunk_hash']
                )
            elif command == 'RETRIEVE':
                response = self.retrieve_chunk(
                    request['filename'],
                    request['chunk_id']
                )
            elif command == 'LIST':
                response = self.list_files()
            elif command == 'DELETE':
                response = self.delete_file(request['filename'])
            else:
                response = {'status': 'error', 'message': 'Unknown command'}
            
            # Send response
            client_socket.sendall(pickle.dumps(response))
        
        except Exception as e:
            error_response = {'status': 'error', 'message': str(e)}
            try:
                client_socket.sendall(pickle.dumps(error_response))
            except:
                pass
        finally:
            client_socket.close()
    
    def store_chunk(self, filename: str, chunk_id: int, chunk_data: bytes, chunk_hash: str):
        """Store a file chunk"""
        try:
            # Create storage path
            file_dir = self.storage_dir / filename
            file_dir.mkdir(exist_ok=True)
            chunk_path = file_dir / f"chunk_{chunk_id}.dat"
            
            # Write chunk to disk
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)
            
            # Update metadata
            if filename not in self.metadata:
                self.metadata[filename] = {}
            self.metadata[filename][chunk_id] = (chunk_hash, str(chunk_path))
            
            return {
                'status': 'success',
                'message': f'Chunk {chunk_id} stored',
                'node_id': self.node_id
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def retrieve_chunk(self, filename: str, chunk_id: int):
        """Retrieve a file chunk"""
        try:
            if filename not in self.metadata or chunk_id not in self.metadata[filename]:
                return {'status': 'error', 'message': 'Chunk not found'}
            
            chunk_hash, chunk_path = self.metadata[filename][chunk_id]
            
            with open(chunk_path, 'rb') as f:
                chunk_data = f.read()
            
            return {
                'status': 'success',
                'chunk_data': chunk_data,
                'chunk_hash': chunk_hash,
                'node_id': self.node_id
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def list_files(self):
        """List all stored files"""
        files_info = {}
        for filename, chunks in self.metadata.items():
            files_info[filename] = list(chunks.keys())
        
        return {
            'status': 'success',
            'files': files_info,
            'node_id': self.node_id
        }
    
    def delete_file(self, filename: str):
        """Delete all chunks of a file"""
        try:
            if filename not in self.metadata:
                return {'status': 'error', 'message': 'File not found'}
            
            # Delete chunks from disk
            for chunk_id, (_, chunk_path) in self.metadata[filename].items():
                if os.path.exists(chunk_path):
                    os.remove(chunk_path)
            
            # Remove directory
            file_dir = self.storage_dir / filename
            if file_dir.exists():
                file_dir.rmdir()
            
            # Remove metadata
            del self.metadata[filename]
            
            return {'status': 'success', 'message': 'File deleted'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def stop(self):
        """Stop the storage node"""
        self.running = False
        if self.socket:
            self.socket.close()


# ============================================================================
# MODULE 3: RETRIEVAL & CONSISTENCY (Client/Coordinator)
# ============================================================================

class DistributedStorageClient:
    """Client that coordinates file storage and retrieval across nodes"""
    
    def __init__(self, replication_factor=2, chunk_size=1024*1024):
        self.replication_factor = replication_factor
        self.partitioner = FilePartitioner(chunk_size=chunk_size)
        self.nodes: List[Dict] = []
        
        # File mapping: {filename: {chunk_id: [node_ids]}}
        self.file_mapping: Dict[str, Dict[int, List[int]]] = {}
    
    def add_node(self, node_id: int, host: str, port: int):
        """Register a storage node"""
        self.nodes.append({
            'node_id': node_id,
            'host': host,
            'port': port
        })
        print(f"[Client] Added node {node_id} at {host}:{port}")
    
    def send_request(self, node: Dict, request: Dict) -> Dict:
        """Send request to a storage node"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((node['host'], node['port']))
            
            # Send request
            sock.sendall(pickle.dumps(request))
            
            # Receive response
            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
                if len(chunk) < 4096:
                    break
            
            sock.close()
            return pickle.loads(data)
        
        except Exception as e:
            return {'status': 'error', 'message': f'Node communication error: {e}'}
    
    def upload_file(self, filepath: str) -> bool:
        """Upload a file with replication"""
        if not self.nodes:
            print("[Client] No storage nodes available")
            return False
        
        filename = os.path.basename(filepath)
        print(f"\n[Client] Uploading {filename}...")
        
        # Partition file
        chunks = self.partitioner.partition_file(filepath)
        print(f"[Client] File split into {len(chunks)} chunks")
        
        # Initialize file mapping
        self.file_mapping[filename] = {}
        
        # Store each chunk with replication
        for chunk_id, chunk_data, chunk_hash in chunks:
            # Select nodes for replication (round-robin)
            num_replicas = min(self.replication_factor, len(self.nodes))
            target_nodes = [
                self.nodes[(chunk_id + i) % len(self.nodes)]
                for i in range(num_replicas)
            ]
            
            stored_nodes = []
            for node in target_nodes:
                request = {
                    'command': 'STORE',
                    'filename': filename,
                    'chunk_id': chunk_id,
                    'chunk_data': chunk_data,
                    'chunk_hash': chunk_hash
                }
                
                response = self.send_request(node, request)
                if response['status'] == 'success':
                    stored_nodes.append(node['node_id'])
                    print(f"[Client] Chunk {chunk_id} stored on node {node['node_id']}")
            
            self.file_mapping[filename][chunk_id] = stored_nodes
        
        print(f"[Client] Upload complete: {filename}")
        return True
    
    def download_file(self, filename: str, output_path: str) -> bool:
        """Download and reassemble a file"""
        if filename not in self.file_mapping:
            print(f"[Client] File {filename} not found")
            return False
        
        print(f"\n[Client] Downloading {filename}...")
        
        chunks = []
        chunk_mapping = self.file_mapping[filename]
        
        # Retrieve each chunk (with fallback to replicas)
        for chunk_id in sorted(chunk_mapping.keys()):
            node_ids = chunk_mapping[chunk_id]
            chunk_retrieved = False
            
            for node_id in node_ids:
                # Find node info
                node = next((n for n in self.nodes if n['node_id'] == node_id), None)
                if not node:
                    continue
                
                request = {
                    'command': 'RETRIEVE',
                    'filename': filename,
                    'chunk_id': chunk_id
                }
                
                response = self.send_request(node, request)
                if response['status'] == 'success':
                    chunk_data = response['chunk_data']
                    chunk_hash = response['chunk_hash']
                    
                    # Verify integrity
                    if self.partitioner.verify_chunk(chunk_data, chunk_hash):
                        chunks.append((chunk_id, chunk_data))
                        print(f"[Client] Retrieved chunk {chunk_id} from node {node_id}")
                        chunk_retrieved = True
                        break
            
            if not chunk_retrieved:
                print(f"[Client] Failed to retrieve chunk {chunk_id}")
                return False
        
        # Reassemble file
        self.partitioner.reassemble_file(chunks, output_path)
        print(f"[Client] Download complete: {output_path}")
        return True
    
    def list_files(self):
        """List all files stored in the system"""
        print("\n[Client] Files in distributed storage:")
        if not self.file_mapping:
            print("  (No files)")
        for filename, chunks in self.file_mapping.items():
            print(f"  - {filename}: {len(chunks)} chunks")
    
    def delete_file(self, filename: str) -> bool:
        """Delete a file from all nodes"""
        if filename not in self.file_mapping:
            print(f"[Client] File {filename} not found")
            return False
        
        print(f"\n[Client] Deleting {filename}...")
        
        # Delete from all nodes
        for node in self.nodes:
            request = {
                'command': 'DELETE',
                'filename': filename
            }
            self.send_request(node, request)
        
        # Remove from mapping
        del self.file_mapping[filename]
        print(f"[Client] Deleted {filename}")
        return True


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def run_storage_node(node_id, host, port, storage_dir):
    """Run a storage node"""
    node = StorageNode(node_id, host, port, storage_dir)
    try:
        node.start()
    except KeyboardInterrupt:
        print(f"\n[Node {node_id}] Shutting down...")
        node.stop()

def run_client_interactive(config_file):
    """Run interactive client"""
    # Load configuration
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    client = DistributedStorageClient(
        replication_factor=config.get('replication_factor', 2),
        chunk_size=config.get('chunk_size', 1024*1024)
    )
    
    # Add nodes from config
    for node_info in config['nodes']:
        client.add_node(node_info['id'], node_info['host'], node_info['port'])
    
    print("\n" + "="*60)
    print("DISTRIBUTED STORAGE CLIENT")
    print("="*60)
    print("Commands:")
    print("  upload <filepath>           - Upload a file")
    print("  download <filename> <dest>  - Download a file")
    print("  list                        - List all files")
    print("  delete <filename>           - Delete a file")
    print("  exit                        - Exit client")
    print("="*60 + "\n")
    
    while True:
        try:
            command = input(">>> ").strip().split()
            if not command:
                continue
            
            cmd = command[0].lower()
            
            if cmd == 'upload' and len(command) == 2:
                client.upload_file(command[1])
            elif cmd == 'download' and len(command) == 3:
                client.download_file(command[1], command[2])
            elif cmd == 'list':
                client.list_files()
            elif cmd == 'delete' and len(command) == 2:
                client.delete_file(command[1])
            elif cmd == 'exit':
                print("Exiting...")
                break
            else:
                print("Invalid command. Use: upload, download, list, delete, exit")
        
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(description='Distributed Cloud File Storage System')
    subparsers = parser.add_subparsers(dest='mode', help='Operation mode')
    
    # Storage node mode
    node_parser = subparsers.add_parser('node', help='Run a storage node')
    node_parser.add_argument('--id', type=int, required=True, help='Node ID')
    node_parser.add_argument('--host', default='localhost', help='Host address')
    node_parser.add_argument('--port', type=int, required=True, help='Port number')
    node_parser.add_argument('--storage', required=True, help='Storage directory')
    
    # Client mode
    client_parser = subparsers.add_parser('client', help='Run interactive client')
    client_parser.add_argument('--config', required=True, help='Configuration file (JSON)')
    
    args = parser.parse_args()
    
    if args.mode == 'node':
        run_storage_node(args.id, args.host, args.port, args.storage)
    elif args.mode == 'client':
        run_client_interactive(args.config)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()