import os
import socket
import threading
import time
import sys
from pathlib import Path
import hashlib
from centralized_handlers import handler as centralized_handler
from decentralized_handlers import handler as decentralized_handler
from static_config import *

class FTPServer:
    def __init__(self, host='127.0.0.1', node_id=0, chord_nodes_config_str=None):
        """
        Initializes the FTPServer instance.
        """
        
        self.utf8_mode = False
        self.host = host
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((host, CONTROL_PORT))
        self.server_socket.listen(5)
        print(f"FTP Server Node {node_id} listening on {host}:{CONTROL_PORT}")

        base_dir = Path(__file__).resolve().parent.parent
        self.resources_dir = base_dir.joinpath(
            f'./server_resources_node_{node_id}')  # Unique resources dir per node

        if not self.resources_dir.exists():
            self.resources_dir.mkdir()
            print(f"Created directory: {self.resources_dir}")
        else:
            print(f"Directory already exists: {self.resources_dir}")
        self.current_dir = self.resources_dir
        print(self.current_dir)

        self.node_id = node_id  # Unique ID for this Chord node
        # self.chord_nodes_config = self.parse_node_config_string(
        #     chord_nodes_config_str)  # Parse string in init - REMOVED
        self.chord_nodes_config = []  # Initialize as empty list - NEW
        self_config = [self.host, CONTROL_PORT,
                       self.node_id]  # Create self config
        self.chord_nodes_config.append(self_config)  # Add self to the list
        # Log it
        print(f"Node {self.node_id} initialized with self config: {self_config}")
        
        self.start_node_discovery_broadcast()  # Start broadcasting - NEW
        self.start_node_discovery_listener()  # Start listener - NEW
        # INTER-NODE SERVER COMPONENTS REMOVED

    def parse_node_config_string(self, config_str):
        """Parses the comma-separated node config string."""
        if not config_str:
            return []
        nodes = []
        node_entries = config_str.split(';')
        for entry in node_entries:
            parts = entry.split(',')
            if len(parts) == 3:
                try:
                    host, port, node_id = parts[0], int(
                        parts[1]), int(parts[2])
                    # Store as list of lists
                    nodes.append([host, int(port), int(node_id)])
                except ValueError:
                    print(
                        f"Warning: Invalid node config entry: {entry}. Skipping.")
            else:
                print(
                    f"Warning: Invalid node config entry format: {entry}. Skipping.")
        return nodes

    def start_node_discovery_broadcast(self):
        """Starts the node discovery broadcast thread."""
        thread = threading.Thread(
            target=self.broadcast_hello_message, daemon=True)
        thread.start()
        print(f"Node Discovery Broadcast thread started.")

    def start_node_discovery_listener(self):
        """Starts the node discovery listener thread."""
        thread = threading.Thread(
            target=self.listen_for_hello_messages, daemon=True)
        thread.start()
        print(f"Node Discovery Listener thread started.")

    def broadcast_hello_message(self):
        """Periodically broadcasts a UDP 'hello' message with node information."""
        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_BROADCAST, 1)  # Enable broadcasting
        # Using '<broadcast>'
        server_address = ('<broadcast>', NODE_DISCOVERY_PORT)

        while True:
            message = f"HELLO,{self.host},{CONTROL_PORT},{self.node_id}"
            try:
                sent = broadcast_socket.sendto(
                    message.encode(), server_address)
                print(
                    f"Node {self.node_id} broadcasted hello message: '{message}'")
            except Exception as e:
                print(
                    f"Error broadcasting hello message from Node {self.node_id}: {e}")
            time.sleep(NODE_DISCOVERY_INTERVAL)  # Broadcast every N seconds

    def listen_for_hello_messages(self):
        """Listens for UDP 'hello' messages from other nodes and updates node config."""
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_address = ("", NODE_DISCOVERY_PORT)  # Bind to all interfaces
        listen_socket.bind(listen_address)
        print(
            f"Node {self.node_id} listening for hello messages on port {NODE_DISCOVERY_PORT}")

        while True:
            try:
                data, address = listen_socket.recvfrom(BUFFER_SIZE)
                message = data.decode().strip()
                if message.startswith("HELLO,"):
                    parts = message.split(',')
                    if len(parts) == 4 and parts[0] == "HELLO":
                        hello_host, hello_control_port_str, hello_node_id_str = parts[
                            1], parts[2], parts[3]
                        try:
                            hello_control_port = int(hello_control_port_str)
                            hello_node_id = int(hello_node_id_str)
                            new_node_info = [hello_host,
                                             hello_control_port, hello_node_id]

                            node_exists = False
                            for existing_node in self.chord_nodes_config:
                                if existing_node[2] == hello_node_id:  # Check by node_id
                                    node_exists = True
                                    break
                            if not node_exists and hello_node_id != self.node_id:  # Avoid adding self and duplicates
                                self.chord_nodes_config.append(new_node_info)
                                print(
                                    f"Node {self.node_id} discovered new node: {new_node_info}")
                                print(
                                    f"Current chord_nodes_config: {self.chord_nodes_config}")
                            else:
                                if hello_node_id != self.node_id:
                                    print(
                                        f"Node {self.node_id} received hello from existing node: {new_node_info} (or self)")

                        except ValueError:
                            print(
                                f"Node {self.node_id} received invalid hello message - ValueError: '{message}' from {address}")
                    else:
                        print(
                            f"Node {self.node_id} received invalid hello message - format error: '{message}' from {address}")
                else:
                    print(
                        f"Node {self.node_id} received non-hello message: '{message}' from {address}")

            except Exception as e:
                print(
                    f"Error listening for hello messages on Node {self.node_id}: {e}")
                
# Inter-node communication (server-to-server) methods - REMOVED

# Chord methods (No changes needed in Chord logic itself)
    def get_key(self, filename):
        """
        Generates a Chord key for a given filename using SHA1 hash.
        """
        return int(hashlib.sha1(filename.encode()).hexdigest(), 16) % 1024

    def find_successor(self, key, nodes_config):
        """
        Finds the successor node for a given key in the Chord ring.
        """
        ring_size = 1024  # Same as key space
        # Sort nodes by node_id
        sorted_nodes = sorted(nodes_config, key=lambda node_info: node_info[2])
        if not sorted_nodes:
            # If no other nodes, current node is the only one
            return [self.host, CONTROL_PORT, self.node_id]

        for i in range(len(sorted_nodes)):
            current_node_info = sorted_nodes[i]
            next_index = (i + 1) % len(sorted_nodes)
            next_node_info = sorted_nodes[next_index]

            current_node_id = current_node_info[2]
            next_node_id = next_node_info[2]

            if current_node_id < next_node_id:  # Normal case, ring not wrapped around
                if current_node_id < key <= next_node_id:
                    return next_node_info
            # Ring wrapped around, e.g., node IDs are [800, 900, 100, 200]
            else:
                if key > current_node_id or key <= next_node_id:  # Key is in the wrap-around range
                    return next_node_info
        # If key is smaller than the smallest node ID, return the first node in the ring
        return sorted_nodes[0]

# File storage/retrieval methods - REMOVED `store_file_on_node`, `retrieve_file_from_node`

    def start(self):
        while True:
            client_sock, addr = self.server_socket.accept()
            print(f"Connection from {addr}")
            threading.Thread(target=centralized_handler.handle_client,
                             args=(client_sock,)).start()

# FTP server methods (rest of FTP server methods remain mostly the same)
    # ... (handle_client, handle_user, handle_pwd, handle_cwd, handle_pasv, handle_list, handle_stor, handle_size, handle_mdtm, handle_mkd, handle_retr, handle_dele, handle_rmd, handle_rnfr, handle_rnto, start) ...


if __name__ == "__main__":
    if len(sys.argv) < 4:  # Expecting node_id, config_string, host_ip
        print("Usage: python server.py <node_id> <node_config_string> <host_ip>")
        print("Example config string: '10.0.11.3,21,0;10.0.11.4,21,1;10.0.11.5,21,2'")
        print("Example run: python server.py 0 '10.0.11.3,21,0;10.0.11.4,21,1;10.0.11.5,21,2' 10.0.11.3")
        sys.exit(1)

    node_id = int(sys.argv[1])
    node_config_str = sys.argv[2]  # Now directly reading string config
    host_ip = sys.argv[3]
    print(f"host_ip from command line: {host_ip}")

    ftp_server = FTPServer(host=host_ip, node_id=node_id,
                           chord_nodes_config_str=node_config_str)  # Pass string
    ftp_server.start()
