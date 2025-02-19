from ast import While
import os
import random
import socket
import tempfile
import threading
import time
import sys
from pathlib import Path
import hashlib
import shutil


CONTROL_PORT = 21
BUFFER_SIZE = 1024
# Port for server-to-server communication (NOT USED NOW)
INTER_NODE_PORT = 5000
CLIENT_HANDLERS_PORT = 5001
CLIENT_HANDLERS_TURN_PORT = 5002
HELLO_PORT = 3000  # Port for UDP global server communication
FILE_REPLICAS_PORT = 3001  # Port for UDP file replicas communication
FOLDER_REPLICAS_PORT = 3002  # Port for UDP folder replicas communication
NODE_DISCOVERY_INTERVAL = 6  # Interval in seconds for broadcasting hello messages
TEMP_DOWNLOAD_DIR_NAME = "temp_downloads"
# Define timeout, e.g., 3 times the broadcast interval
INACTIVE_NODE_TIMEOUT = 10  # Timeout in seconds for removing inactive nodes
REPLICATION_FACTOR = 3
REDISTRIBUTION_INTERVAL = 10  # Interval in seconds for redistributing replicas


class FTPServer:
    def __init__(self, host='127.0.0.1', node_id=0):
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

        self.node_id = node_id  # Unique ID for this Chord node
        self.am_i_bradcsting_redist_turn=False
        if not self.resources_dir.exists():
            self.resources_dir.mkdir()
            print(f"Created directory: {self.resources_dir}")
        else:
            print(f"Directory already exists: {self.resources_dir}")
            if self.node_id!=1:
                self.clean_all_data_in_resources_dir()
        self.current_dir = self.resources_dir
        print(self.current_dir)

        self.discovery_timer = None  # To hold the timer object

        self.chord_nodes_config = []  # Initialize as empty list
        self_config = [self.host, CONTROL_PORT,
                       self.node_id, time.time()]  # timestamp
        #initializate the list of nodes talking to client that accept by default four argument without inserting any element
        self.nodes_talking_to_client = []
        
        self.chord_nodes_config.append(self_config)
        # Dictionary to track folder replicas: {Key: Full Path of Folder , Value: [node_id1, node_id2, node_id3]}
        # self.folder_replicas = {}
        self.start_listeners()
        self.start_node_discovery_broadcast()

        # Dictionary to track file replicas: {Key: Full Path of File including filename , Value: [node_id1, node_id2, node_id3]}
        # self.file_replicas = {}
        # Broadcast the updated file_replicas to other nodes
        # self.broadcast_file_replicas_merge()

        # Broadcast the updated file_replicas to other nodes
        # self.broadcast_folder_replicas_merge()

        # Log it
        print(f"Node {self.node_id} initialized with self config: {self_config}")
        self.start_inactive_node_removal_thread()
        self.who_is_next()

    # region COMMS
    def clean_all_data_in_resources_dir(self):
        """
        Cleans the resources directory by deleting all files and folders.
        """
        for root, dirs, files in os.walk(self.resources_dir, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                try:
                    os.rmdir(dir_path)
                except OSError as e:
                    # Optional error handling
                    print(f"Error removing directory {dir_path}: {e}")

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

    def start_node_client_handler_broadcast(self):
        """Starts the node discovery broadcast thread."""
        thread = threading.Thread(
            target=self.broadcast_hello_client_handler_message, daemon=True)
        thread.start()
        print(f"Node Discovery Broadcast thread started.")

    def start_inactive_node_removal_thread(self):
        """Starts the inactive node removal loop in a separate thread."""
        thread = threading.Thread(
            target=self.run_inactive_node_removal_loop, daemon=True)
        thread.start()
        print(
            f"Inactive Node Removal thread started. Checking every {INACTIVE_NODE_TIMEOUT} seconds.")

    def start_listeners(self):
        """Starts the listener threads."""
        threading.Thread(target=self.listen_for_hello_messages,
                         daemon=True).start()
        threading.Thread(
            target=self.listen_for_client_handlers, daemon=True).start()
        threading.Thread(
            target=self.listen_for_client_handler_turn, daemon=True).start()
        
        print(f"Node Discovery Listener threads started.")

    def broadcast_hello_message(self):
        """Periodically broadcasts a UDP 'hello' message with node information."""
        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_BROADCAST, 1)  # Enable broadcasting
        broadcast_socket.bind(('', 0))  # Bind to any available port
        # Using '<broadcast>'
        server_address = ('<broadcast>', HELLO_PORT)

        while True:
            message = f"HELLO,{self.host},{CONTROL_PORT},{self.node_id}"
            try:
                sent = broadcast_socket.sendto(
                    message.encode(), server_address)
                # print(
                #     f"Node {self.node_id} broadcasted hello message: '{message}'")
            except Exception as e:
                print(
                    f"Error broadcasting hello message from Node {self.node_id}: {e}")
            time_random = random.uniform(0.1, NODE_DISCOVERY_INTERVAL)
            time.sleep(time_random)  # Broadcast every N seconds

    def broadcast_client_handler_turn(self):
        """Periodically broadcasts a UDP 'hello' message with node information."""
        
        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_BROADCAST, 1)  # Enable broadcasting
        broadcast_socket.bind(('', 0))  # Bind to any available port
        # Using '<broadcast>'
        server_address = ('<broadcast>', CLIENT_HANDLERS_TURN_PORT)
        counter=0
        while True:
            time.sleep(REDISTRIBUTION_INTERVAL)  # Broadcast every N seconds
            # only the node with the highest id will broadcast this message
            highest_node_id = max([node[2]
                                  for node in self.chord_nodes_config])
            if highest_node_id != self.node_id:
                return
            if counter>=len(self.chord_nodes_config):
                counter=0
            is_turn_of_node_id = self.chord_nodes_config[counter][2]
            message = f"TURN OF:{is_turn_of_node_id}"
            try:
                sent = broadcast_socket.sendto(
                    message.encode(), server_address)
                counter+=1
                
            except Exception as e:
                print(
                    f"Error broadcasting TURN message from Node {self.node_id}: {e}")
                
    def start_broadcast_client_handler_turn(self):
        thread = threading.Thread(
            target=self.broadcast_client_handler_turn, daemon=True)
        thread.start()
        print(f"Client Handler Turn Loop started.")
    def who_is_next(self):
            highest_node_id = max([node[2]
                                  for node in self.chord_nodes_config])
            if self.node_id == highest_node_id and not self.am_i_bradcsting_redist_turn:
                self.am_i_bradcsting_redist_turn=True
                self.start_broadcast_client_handler_turn()
            
            
    #listener of the turn of the client handler
    def listen_for_client_handler_turn(self):
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_address = ("", CLIENT_HANDLERS_TURN_PORT)
        listen_socket.bind(listen_address)

        while True:
            try:
                data, address = listen_socket.recvfrom(BUFFER_SIZE)
            
                message = data.decode().strip()
                if message.startswith("TURN OF:"):
                    parts = message.split(':')
                    if len(parts) == 2 and parts[0] == "TURN OF":
                        is_turn_of_node_id_str = parts[1]
                        try:
                            is_turn_of_node_id = int(is_turn_of_node_id_str)
                            if is_turn_of_node_id==self.node_id:
                                self.start_replicate_thread()
                        except ValueError:
                            print(
                                f"Node {self.node_id} received invalid TURN message - ValueError: '{message}' from {address}")
                    else:
                        print(
                            f"Node {self.node_id} received invalid TURN message - format error: '{message}' from {address}")

            except Exception as e:
                print(
                    f"Error listening for TURN messages on Node {self.node_id}: {e}")
    def broadcast_hello_client_handler_message(self):
        """Periodically broadcasts a UDP 'hello' message with node information."""
        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_BROADCAST, 1)  # Enable broadcasting
        broadcast_socket.bind(('', 0))  # Bind to any available port
        # Using '<broadcast>'
        server_address = ('<broadcast>', CLIENT_HANDLERS_PORT)

        while True:
            message = f"CLIENT_H,{self.host},{CONTROL_PORT},{self.node_id}"
            try:
                sent = broadcast_socket.sendto(
                    message.encode(), server_address)
                # print(
                #     f"Node {self.node_id} broadcasted hello message: '{message}'")
            except Exception as e:
                print(
                    f"Error broadcasting CLIENT_H message from Node {self.node_id}: {e}")
            time_random = random.uniform(0.1, 2)
            time.sleep(time_random)  # Broadcast every N seconds

    def listen_for_client_handlers(self):
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_address = ("", CLIENT_HANDLERS_PORT)
        listen_socket.bind(listen_address)

        while True:
            try:
                data, address = listen_socket.recvfrom(BUFFER_SIZE)
                ip, port = address  # ip and self.host has format x.x.x.x
                if ip == self.host:
                    continue
                message = data.decode().strip()
                if message.startswith("CLIENT_H,"):
                    parts = message.split(',')
                    if len(parts) == 4 and parts[0] == "CLIENT_H":
                        hello_host, hello_control_port_str, hello_node_id_str = parts[
                            1], parts[2], parts[3]
                        try:
                            hello_control_port = int(hello_control_port_str)
                            hello_node_id = int(hello_node_id_str)
                            new_node_info = [hello_host,
                                             hello_control_port, hello_node_id]

                            node_exists = False
                            for existing_node in self.chord_nodes_config:
                                if existing_node[2] == hello_node_id:
                                    node_exists = True
                                    if len(existing_node) < 4:
                                        print(
                                            f"Warning: Node config for node {hello_node_id} is missing timestamp. Adding current timestamp.")
                                        existing_node.append(time.time())
                                    else:
                                        existing_node[3] = time.time()
                                    break

                            if not node_exists and hello_node_id != self.node_id:
                                new_node_info.append(time.time())
                                self.start_replicate_thread()
                                # time.sleep(5)
                                self.nodes_talking_to_client.append(
                                    new_node_info)
                                
                                
                                print(
                                    f"Node {self.node_id} discovered new client handler: {new_node_info}")
                                print(
                                    f"Current nodes talking with clients: {self.chord_nodes_config}")

                        except ValueError:
                            print(
                                f"Node {self.node_id} received invalid CLIENT_H message - ValueError: '{message}' from {address}")
                    else:
                        print(
                            f"Node {self.node_id} received invalid CLIENT_H message - format error: '{message}' from {address}")

            except Exception as e:
                print(
                    f"Error listening for CLIENT_H messages on Node {self.node_id}: {e}")
                
    def start_replicate_thread(self):
        thread = threading.Thread(
            target=self.replicate_data_to_all_nodes, daemon=True)
        thread.start()
        
    def reset_discovery_timer(self):
        """Resets the discovery timer. Cancels any existing timer and starts a new one."""
        if self.discovery_timer:
            self.discovery_timer.cancel()  # Cancel the existing timer if running

        self.discovery_timer = threading.Timer(
            5, self.replicate_data_to_all_nodes)  # Create a new timer
        self.discovery_timer.start()  # Start the new timer
        print(f"Node {self.node_id}: Timer reset and started for 5 seconds.")

    def listen_for_hello_messages(self):
        """Listens for UDP 'hello' messages from other nodes and updates node config."""
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_address = ("", HELLO_PORT)
        listen_socket.bind(listen_address)

        while True:
            try:
                data, address = listen_socket.recvfrom(BUFFER_SIZE)
                ip, port = address  # ip and self.host has format x.x.x.x
                if ip == self.host:
                    continue
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
                                if existing_node[2] == hello_node_id:
                                    node_exists = True
                                    if len(existing_node) < 4:
                                        print(
                                            f"Warning: Node config for node {hello_node_id} is missing timestamp. Adding current timestamp.")
                                        existing_node.append(time.time())
                                    else:
                                        existing_node[3] = time.time()
                                    break

                            if not node_exists and hello_node_id != self.node_id:
                                new_node_info.append(time.time())
                                self.chord_nodes_config.append(new_node_info)
                                # self.who_is_next()
                                # Reset and start the timer every time a new node is discovered
                                # self.reset_discovery_timer()
                                # self.broadcast_file_replicas_merge()
                                # self.broadcast_folder_replicas_merge()
                                print(
                                    f"Node {self.node_id} discovered new node: {new_node_info}")
                                print(
                                    f"Current chord_nodes_config: {self.chord_nodes_config}")

                        except ValueError:
                            print(
                                f"Node {self.node_id} received invalid hello message - ValueError: '{message}' from {address}")
                    else:
                        print(
                            f"Node {self.node_id} received invalid hello message - format error: '{message}' from {address}")

            except Exception as e:
                print(
                    f"Error listening for hello messages on Node {self.node_id}: {e}")

    def run_inactive_node_removal_loop(self):
        """
        Runs a loop that periodically checks and removes inactive nodes
        using time.sleep.
        """
        print("Inactive Node Removal loop started.")
        while True:
            time.sleep(INACTIVE_NODE_TIMEOUT)  # Wait for timeout period
            self.remove_inactive_nodes()

    def remove_inactive_nodes(self):
        """Removes inactive nodes from chord_nodes_config based on last hello message time."""
        # print("Checking for inactive nodes...")
        current_time = time.time()
        removed_nodes = []
        for node_info in self.chord_nodes_config:
            node_id = node_info[2]
            last_seen_time = node_info[3]
            if node_id == self.node_id:  # Don't remove self
                continue

            if current_time - last_seen_time > INACTIVE_NODE_TIMEOUT:
                print(
                    f"Inactive node found: Node {node_info[2]} (last seen: {node_info[3]})")

                removed_nodes.append(node_info)

        if removed_nodes:
            highest_node_id = max([node[2]
             for node in self.chord_nodes_config])
            
            for node in removed_nodes:
                self.chord_nodes_config.remove(node)
                if node in self.nodes_talking_to_client:
                    self.nodes_talking_to_client.remove(node)
                # remove node id from file_replicas and folder_replicas
                # node_id = node[2]
                # self.remove_node_from_file_replicas(node_id)
                # self.remove_node_from_folder_replicas(node_id)
            if highest_node_id not in [node[2] for node in self.chord_nodes_config]:
                self.who_is_next()
            print(f"Removed inactive nodes: {removed_nodes}")

            print(
                f"Updated chord_nodes_config after remove inactive nodes: {self.chord_nodes_config}")
            # print(f"Current file_replicas: {self.file_replicas}")
            # print(f"Current folder_replicas: {self.folder_replicas}")
            # self.redistribute_replicas(removed_nodes)

    def replicate_data_to_all_nodes(self):
        """
        Replicates all files and folders from this node to all other nodes
        in the chord configuration.

        Assumptions:
        - self.chord_config is a list of tuples, where each tuple represents a node
        and contains node information, and node[2] is the node_id.
        - self.resources_dir is the root directory of the resources to be replicated.
        - self.copy_file_to_node(file_path, target_node_info) is a method that
        handles copying a file to another node.
        - self.create_folder_on_node(folder_path, target_node_info) is a method that
        handles creating a folder on another node.
        - These methods should handle the actual network transfer and any necessary
        communication with the target nodes.
        """
        # sorted_nodes = sorted(self.chord_nodes_config, key=lambda x: x[2])
        #get the last node of sorted_nodes
        
        print("Starting full data replication to all nodes...")
        # get random index for sorted(self.nodes_talking_to_client, key=lambda x: x[2])
        # random_index=random.randint(0,len(self.nodes_talking_to_client)-1)
        # random_node=sorted(self.nodes_talking_to_client, key=lambda x: x[2])[random_index]
        # if self.node_id!=random_node[2]:
        #     return
        # Identify target nodes (all nodes in chord_config EXCEPT self)
        target_nodes = [
            node for node in self.chord_nodes_config if node[2] != self.node_id]

        if not target_nodes:
            print(
                "No other nodes to replicate data to. Only one node in the configuration (or only self).")
            return

        # print(f"Target nodes for replication: {[node[2] for node in target_nodes]}")

        # Walk through the resources directory and replicate files and folders
        for root, directories, files in os.walk(self.resources_dir):
            relative_root = os.path.relpath(
                root, self.resources_dir) if root != self.resources_dir else ""  # Handle root directory
            # skip TEMP_DOWNLOAD_DIR_NAME directory
            if relative_root == TEMP_DOWNLOAD_DIR_NAME:
                continue
            for directory in directories:
                relative_folder_path = os.path.join(relative_root, directory)
                # print(f"Replicating folder: '{relative_folder_path}'")

                for target_node_info in target_nodes:
                    target_node_id = target_node_info[2]
                    # print(f"  -> to node {target_node_id}...")
                    try:
                        if not self.create_folder_on_node(relative_folder_path, target_node_info):
                            print(
                                f"  Failed to create folder '{relative_folder_path}' on node {target_node_id}.")
                        else:
                            a=1
                            # print(
                                # f"  Folder '{relative_folder_path}' created on node {target_node_id} successfully.")
                    except Exception as e:
                        print(
                            f"  Error replicating folder '{relative_folder_path}' to node {target_node_id}: {e}")

            for filename in files:
                relative_file_path = os.path.join(relative_root, filename)
                # print(f"Replicating file: '{relative_file_path}'")

                for target_node_info in target_nodes:
                    target_node_id = target_node_info[2]
                    # print(f"  -> to node {target_node_id}...")
                    try:
                        if not self.copy_file_to_node(relative_file_path, target_node_info):
                            print(
                                f"  Failed to copy file '{relative_file_path}' to node {target_node_id}.")
                        else:
                            a=1
                            # print(
                            #     f"  File '{relative_file_path}' copied to node {target_node_id} successfully.")
                    except Exception as e:
                        print(
                            f"  Error replicating file '{relative_file_path}' to node {target_node_id}: {e}")

        print("Full data replication to all nodes completed.")
        self.discovery_timer = None

    def create_folder_on_node(self, folder_path, node_info):
        """Creates a folder in the specified node."""
        print(
            f"NODE {self.node_id} : COPING {folder_path} to node {node_info[2]} .")
        # self.chord_nodes_config = []  # Initialize as empty list self_config = [self.host, CONTROL_PORT, self.node_id, time.time()] self.chord_nodes_config.append(self_config)

        node_ip, node_control_port, node_id, node_time = node_info  # Unpack node_info
        timeout_seconds = 15
        try:
            ftp_client_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            # Set timeout for client socket operations
            ftp_client_socket.settimeout(timeout_seconds)
            ftp_client_socket.connect(
                (node_ip, node_control_port))
            ftp_client_socket.recv(
                BUFFER_SIZE)  # Welcome message
            ftp_client_socket.send(b"PASV\r\n")
            pasv_response = ftp_client_socket.recv(
                BUFFER_SIZE).decode()
            ip_str = pasv_response.split('(')[1].split(')')[0]
            ip_parts = ip_str.split(',')
            data_server_ip = ".".join(ip_parts[:4])
            data_server_port = (
                int(ip_parts[4]) << 8) + int(ip_parts[5])
            print(
                f"Node {self.node_id}: Data server IP: {data_server_ip}, port: {data_server_port}")
            ftp_data_socket_client = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            # Set timeout for data socket operations
            ftp_data_socket_client.settimeout(timeout_seconds)
            ftp_data_socket_client.connect(
                (data_server_ip, data_server_port))

            ftp_client_socket.send(f"MAKEDIR {folder_path}\r\n".encode())

            ftp_data_socket_client.close()

            try:
                response = ftp_client_socket.recv(
                    BUFFER_SIZE).decode()  # Get 123 or error
            except socket.timeout:  # Timeout during recv of response from control socket
                print(
                    f"Node {self.node_id}: Timeout waiting for COPY response from node {node_id}.")
                ftp_client_socket.close()
                return False

            ftp_client_socket.close()
            print(f"Node {self.node_id}: Folder creation RESPONSE: {response}")
            if response.startswith("123"):
                print(
                    f"REPLICATION OF {folder_path} TO NODE {node_id} SUCCESSFULLY.")
                return True
            else:
                print(
                    f"REPLICATION OF {folder_path} TO NODE {node_id} ERRORRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR.")
                return False
        except socket.timeout:
            print(
                f"Node {self.node_id}: Timeout occurred while communicating with node {node_id}.")
            # You might want to close sockets here if they were successfully created
            if 'ftp_client_socket' in locals():
                ftp_client_socket.close()
            if 'ftp_data_socket_client' in locals():
                ftp_data_socket_client.close()
            return False
        except Exception as e:
            print(
                f"Error copying folder to node {node_id}: {e}")
            # You might want to close sockets here if they were successfully created
            if 'ftp_client_socket' in locals():
                ftp_client_socket.close()
            if 'ftp_data_socket_client' in locals():
                ftp_data_socket_client.close()
            return False

    def receive_folder_creation_order(self, client_socket, data_socket, folder_path):
        final_folder_path = os.path.join(
            self.resources_dir, folder_path)
        if not os.path.exists(final_folder_path):
            try:
                os.makedirs(final_folder_path)
                client_socket.send(b"123 Folder created successfully.\r\n")
            except OSError as exc:
                print(f"Error creating directory: {exc}")
                client_socket.send(b"451 Error creating folder.\r\n")
        else:
            client_socket.send(b"123 Folder already exists.\r\n")

    def copy_file_to_node(self, file_path, node_info):
        """Creates a folder in the specified node."""
        print(
            f"NODE {self.node_id} : COPING {file_path} to node {node_info[2]} .")
        # self.chord_nodes_config = []  # Initialize as empty list self_config = [self.host, CONTROL_PORT, self.node_id, time.time()] self.chord_nodes_config.append(self_config)

        node_ip, node_control_port, node_id, node_time = node_info  # Unpack node_info
        timeout_seconds = 15
        try:
            ftp_client_socket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            # Set timeout for client socket operations
            ftp_client_socket.settimeout(timeout_seconds)
            ftp_client_socket.connect(
                (node_ip, node_control_port))
            ftp_client_socket.recv(
                BUFFER_SIZE)  # Welcome message
            ftp_client_socket.send(b"PASV\r\n")
            pasv_response = ftp_client_socket.recv(
                BUFFER_SIZE).decode()
            ip_str = pasv_response.split('(')[1].split(')')[0]
            ip_parts = ip_str.split(',')
            data_server_ip = ".".join(ip_parts[:4])
            data_server_port = (
                int(ip_parts[4]) << 8) + int(ip_parts[5])
            print(
                f"Node {self.node_id}: Data server IP: {data_server_ip}, port: {data_server_port}")
            ftp_data_socket_client = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            # Set timeout for data socket operations
            ftp_data_socket_client.settimeout(timeout_seconds)
            ftp_data_socket_client.connect(
                (data_server_ip, data_server_port))

            ftp_client_socket.send(f"COPY {file_path}\r\n".encode())
            full_file_path = os.path.join(self.resources_dir, file_path)
            print("full_file_path", full_file_path)
            with open(full_file_path, 'rb') as temp_file_forward:
                while True:
                    data_to_forward = temp_file_forward.read(
                        BUFFER_SIZE)
                    if not data_to_forward:
                        break
                try:
                    ftp_data_socket_client.sendall(
                        data_to_forward)
                except socket.timeout:  # Timeout during sendall on data socket
                    print(
                        f"Node {self.node_id}: Timeout during sending data to node {node_id}.")
                    ftp_data_socket_client.close()
                    ftp_client_socket.close()
                    return False

            ftp_data_socket_client.close()

            try:
                response = ftp_client_socket.recv(
                    BUFFER_SIZE).decode()  # Get 226 or error
            except socket.timeout:  # Timeout during recv of response from control socket
                print(
                    f"Node {self.node_id}: Timeout waiting for COPY response from node {node_id}.")
                ftp_client_socket.close()
                return False

            ftp_client_socket.close()
            print(f"Node {self.node_id}: COPY RESPONSE: {response}")
            if response.startswith("226"):
                print(
                    f"REPLICATION OF {file_path} TO NODE {node_id} SUCCESSFULLY.")
                return True
            else:
                print(
                    f"REPLICATION OF {file_path} TO NODE {node_id} ERRORRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR.")
                return False
        except socket.timeout:
            print(
                f"Node {self.node_id}: Timeout occurred while communicating with node {node_id}.")
            # You might want to close sockets here if they were successfully created
            if 'ftp_client_socket' in locals():
                ftp_client_socket.close()
            if 'ftp_data_socket_client' in locals():
                ftp_data_socket_client.close()
            return False
        except Exception as e:
            print(
                f"Error copying file to node {node_id}: {e}")
            # You might want to close sockets here if they were successfully created
            if 'ftp_client_socket' in locals():
                ftp_client_socket.close()
            if 'ftp_data_socket_client' in locals():
                ftp_data_socket_client.close()
            return False

    def receive_file(self, client_socket, data_socket, file_path):
        """
        Receives a file from a client and saves it to the server.
        """
        print(f"Node {self.node_id}: Receiving file: {file_path}")
        # Ensure temp download directory exists
        temp_dir_path = self.resources_dir.joinpath(
            TEMP_DOWNLOAD_DIR_NAME)
        if not temp_dir_path.exists():
            temp_dir_path.mkdir(parents=True, exist_ok=True)

        # get filename from file_path example: file_path = "candelasa/mongo.txt" and filename should be  mongo.txt , other example file_path = "loco.mp4" and filename should be "loco.mp4"

        # key = self.get_key(file_path)
        random_number = random.randint(0, 10000)
        temp_file_path = temp_dir_path.joinpath(f"temp_{random_number}")
        print(
            f"Node {self.node_id}: Storing data to temp file: {temp_file_path}")

        try:
            conn, addr = data_socket.accept()
            with open(temp_file_path, 'wb') as temp_file:
                while True:
                    data = conn.recv(BUFFER_SIZE)
                    if not data:
                        break
                    temp_file.write(data)
            conn.close()
            print(f"Node {self.node_id}: Finished receiving data to temp file.")
        except Exception as e:
            print(f"Error receiving file data: {e}")
            client_socket.send(b"550 Failed to receive file data.\r\n")
            return

        print(
            f"Node {self.node_id}: FILE REPLICATED, and this node is responsible, storing locally from temp.")
        try:
            final_file_path = os.path.join(
                self.resources_dir, file_path)
            # check if exist the directory of final_file_path
            if not os.path.exists(os.path.dirname(final_file_path)):
                try:
                    os.makedirs(os.path.dirname(final_file_path))
                except OSError as exc:
                    print(f"Error creating directory: {exc}")
            shutil.copy2(temp_file_path, final_file_path)
            print(
                f"Node {self.node_id}: Successfully stored forwarded file locally: {file_path}")
            client_socket.send(b"226 File stored successfully.\r\n")
        except Exception as e:
            print(
                f"Node {self.node_id}: Error storing forwarded file locally: {e}")
            # Consider better error reporting
            client_socket.send(
                b"550 Failed to store forwarded file locally.\r\n")
        finally:
            if data_socket:
                data_socket.close()
            try:
                # Clean up temp file after attempts to distribute (success or failure)
                os.remove(temp_file_path)
                print(
                    f"Node {self.node_id}: Temp file removed: {temp_file_path}")
            except Exception as e:
                print(
                    f"Node {self.node_id}: Error deleting temp file {temp_file_path}: {e}")


# endregion COMMS

# Chord methods (No changes needed in Chord logic itself)

# Decentralized FTP server methods

    def decentralized_handle_dele(self, client_socket, filename, virtual_current_dir):
        """
        Handles the FORWARD_DELE command to delete a file locally.
        """
        try:
            if str(virtual_current_dir)[0] == "/":
                        virtual_current_dir = str(virtual_current_dir)[1:]
            print(f"virtual_current_dir: {virtual_current_dir}")
            #virtual_current_directory is a directory relative to self.current_dir
            file_path = os.path.join(self.current_dir, virtual_current_dir)
            print(f"file_path: {file_path}")
            os.remove(os.path.join(file_path, filename))
            client_socket.send(b"250 File deleted successfully.\r\n")
        except Exception as e:
            print(f"Error in DELE: {e}")
            client_socket.send(b"550 Failed to delete file.\r\n")
        # try:
        #     virtual_current_dir = self.current_dir
        #     if str(virtual_current_dir)[0] == "/":
        #         virtual_current_dir = str(virtual_current_dir)[1:]

        #     file_path = os.path.join(os.path.join(
        #         self.resources_dir, virtual_current_dir), filename)
        #     relative_file_path = os.path.join(virtual_current_dir, filename)
        #     if os.path.exists(file_path):
        #         os.remove(file_path)
        #         client_socket.send(
        #             b"250 File deleted successfully (forwarded).\r\n")
        #     else:
        #         client_socket.send(b"550 File not found (forwarded).\r\n")
        # except Exception as e:
        #     print(f"Error in FORWARD_DELE: {e}")
        #     client_socket.send(b"550 Failed to delete file (forwarded).\r\n")

    def decentralized_handle_rmdir(self, client_socket, dirname, virtual_current_dir):
        """
        Handles the FORWARD_RMDIR command to delete a file locally.
        """
        try:
            if str(virtual_current_dir)[0] == "/":
                        virtual_current_dir = str(virtual_current_dir)[1:]
            #adding logs
            print(f"virtual_current_dir: {virtual_current_dir}")
            #virtual_current_directory is a directory relative to self.current_dir
            folder_path = os.path.join(self.current_dir, virtual_current_dir)
            print(f"folder_path: {folder_path}")
            os.rmdir(os.path.join(folder_path, dirname))
            client_socket.send(b"250 Directory deleted successfully.\r\n")
        except Exception as e:
            print(f"Error in RMD: {e}")
            client_socket.send(b"550 Failed to delete directory.\r\n")
        # try:
        #     virtual_current_dir= self.current_dir
        #     if str(virtual_current_dir)[0] == "/":
        #         virtual_current_dir = str(virtual_current_dir)[1:]

        #     folder_path = os.path.join(os.path.join(
        #         self.resources_dir, virtual_current_dir), dirname)
        #     relative_folder_path = os.path.join(virtual_current_dir, dirname)
        #     if os.path.exists(folder_path):
        #         os.rmdir(folder_path)
        #         client_socket.send(
        #             b"250 Folder deleted successfully (forwarded).\r\n")
        #     else:
        #         client_socket.send(b"550 Folder not found (forwarded).\r\n")
        # except Exception as e:
        #     print(f"Error in FORWARD_RMDIR: {e}")
        #     client_socket.send(b"550 Failed to delete folder (forwarded).\r\n")

# FTP server methods (rest of FTP server methods remain mostly the same)
    # ... (handle_client, handle_user, handle_pwd, handle_cwd, handle_pasv, handle_list, handle_stor, handle_size, handle_mdtm, handle_mkd, handle_retr, handle_dele, handle_rmd, handle_rnfr, handle_rnto, start) ...

    def handle_client(self, client_socket,addr):
        try:
            client_socket.send(b"220 Welcome to the FTP server.\r\n")
            data_socket = None
            talking_to_real_client=False
            while True:
                try:
                    if addr[0].startswith("10.0.10.") and not talking_to_real_client:
                        talking_to_real_client=True
                        self.nodes_talking_to_client.append([self.host, CONTROL_PORT, self.node_id, time.time()])
                        # self.start_node_client_handler_broadcast()
                    
                    command = client_socket.recv(BUFFER_SIZE).decode().strip()
                    if not command:
                        break
                    print(f"Received command: {command}")
                    cmd = command.split()[0].upper()
                    arg = command[len(cmd):].strip()
                    print(f"Command: {cmd}, Argument: {arg}")
                    if cmd == "USER":
                        res = self.handle_user(client_socket, arg)
                        client_socket.send(res)
                    elif cmd == "COPY":
                        self.receive_file(client_socket, data_socket, arg)
                    elif cmd == "PASS":
                        client_socket.send(b"230 User logged in, proceed.\r\n")
                    elif cmd == "SYST":
                        client_socket.send(b"215 UNIX Type: L8\r\n")
                    elif cmd == "FEAT":
                        client_socket.send(
                            b"211-Features:\r\n UTF8\r\n211 End\r\n")
                    elif cmd == "PWD":
                        self.handle_pwd(client_socket)
                    elif cmd == "CWD":
                        self.handle_cwd(client_socket, arg)
                    elif cmd == "CDUP":
                        self.handle_cwd(client_socket, Path(
                            self.current_dir).parent)
                    elif cmd == "TYPE":
                        client_socket.send(b"200 Type set to I.\r\n")
                    elif cmd == "PASV":
                        data_socket = self.handle_pasv(client_socket)
                    elif cmd == "LIST":
                        self.handle_list(client_socket, data_socket)
                        
                    elif cmd == "STOR":
                        self.handle_stor(client_socket, data_socket, arg)
                        
                        self.start_replicate_thread()
                        time.sleep(3)
                    elif cmd == "FORWARD_DELE":
                        filename, virtual_current_dir = self.parse_path_argument(
                            arg)
                        self.decentralized_handle_dele(
                            client_socket, filename, virtual_current_dir)
                    elif cmd == "FORWARD_RMDIR":
                        dirname, virtual_current_dir = self.parse_path_argument(
                            arg)
                        self.decentralized_handle_rmdir(
                            client_socket, dirname, virtual_current_dir)
                    elif cmd == "MAKEDIR":
                        self.receive_folder_creation_order(
                            client_socket, data_socket, arg)
                        
                    elif cmd == "SIZE":
                        self.handle_size(client_socket, arg)
                    elif cmd == "MDTM":
                        self.handle_mdtm(client_socket, arg)
                    elif cmd == "MKD":
                        self.handle_mkd(client_socket, arg)
                        self.start_replicate_thread()
                        time.sleep(3)
                    elif cmd == "RETR":
                        self.handle_retr(client_socket, data_socket, arg)
                    elif cmd == "DELE":
                        self.handle_dele(client_socket, arg)
                    elif cmd == "RMD":
                        self.handle_rmd(client_socket, arg)
                    elif cmd == "RNFR":
                        rename_from = self.handle_rnfr(client_socket, arg)
                    elif cmd == "RNTO":
                        self.handle_rnto(client_socket, rename_from, arg)
                    elif cmd == "QUIT":
                        client_socket.send(b"221 Goodbye.\r\n")
                    # elif cmd == "AUTH":
                    #     self.handle_auth(client_socket, arg)
                    # elif cmd == "OPTS":
                    #     self.handle_opts(client_socket, arg)
                    #     break
                    else:
                        client_socket.send(
                            b"502 Command: %s not implemented.\r\n" % cmd.encode())
                except ConnectionResetError:
                    print(
                        "An existing connection was forcibly closed by one remote host .")
                    break
        finally:
            client_socket.close()
            if data_socket:
                data_socket.close()

    def parse_path_argument(self, arg):
        """
        Parses the argument string to extract the filename and virtual current directory.
        Ensures virtual_current_dir is never "", defaulting to "/" if no directory part.
        Args:
            arg: The path argument string (e.g., "/games/Batman AK/batman.exe").
        Returns:
            A tuple containing:
                - filename: The filename (e.g., "batman.exe").
                - virtual_current_dir: The virtual current directory (e.g., "/games/Batman AK", or "/" if no dir).
        """
        filename = os.path.basename(arg)
        virtual_current_dir = os.path.dirname(arg)

        # Handle cases where dirname returns empty string (no directory part) or "." (current dir)
        if virtual_current_dir == "" or virtual_current_dir == ".":
            # Default to "/" when no directory part is found.
            virtual_current_dir = "/"
        elif virtual_current_dir == "/":  # dirname returns "/" for paths like "/filename.txt"
            pass  # virtual_current_dir is already correctly "/"
        else:
            pass  # dirname returned a valid directory path

        return filename, virtual_current_dir

    def handle_dele(self, client_socket, filename):
        try:
            os.remove(os.path.join(self.current_dir, filename))
            print(f"Self.current_dir: {self.current_dir}")
            client_socket.send(b"250 File deleted successfully.\r\n")
            for node_info in self.chord_nodes_config:
                if self.node_id == node_info[2]:
                    continue
                target_host = node_info[0]
                target_port = node_info[1]
                target_id = node_info[2]
                forward_socket = None
                try:
                    forward_socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM)
                    forward_socket.connect((target_host, CONTROL_PORT))
                    welcome_message = forward_socket.recv(BUFFER_SIZE)
                    relative_path_to_curr_dir= ""
                    parts = str(self.current_dir).split('/')

                    # Assuming the part you want to remove is always the second element (index 1)
                    # and you want to keep everything from the third element onwards (index 2)
                    extracted_parts = parts[2:]

                    extracted_path = "/" + "/".join(extracted_parts)
                    command_to_send = f"FORWARD_DELE {os.path.join(extracted_path, filename)}"
                    forward_socket.send(command_to_send.encode())
                    response = forward_socket.recv(BUFFER_SIZE).decode().strip()
                    if response.startswith("250"):
                        print(
                            f"FORWARD_DELE to node {target_id} successful.")
                    else:
                        print(
                            f"FORWARD_DELE to node {target_id} failed with response: {response}")
                except Exception as e:
                    print(f"Error forwarding DELE to node {target_id}: {e}")
                finally:
                    if forward_socket:
                        forward_socket.close()
        except Exception as e:
            print(f"Error in DELE: {e}")
            client_socket.send(b"550 Failed to delete file.\r\n")

    def handle_rmd(self, client_socket, dirname):
        try:
            os.rmdir(os.path.join(self.current_dir, dirname))
            print(f"Self.current_dir: {self.current_dir}")
            client_socket.send(b"250 Directory deleted successfully.\r\n")
            for node_info in self.chord_nodes_config:
                if self.node_id == node_info[2]:
                    continue
                target_host = node_info[0]
                target_port = node_info[1]
                target_id = node_info[2]
                forward_socket = None
                try:
                    forward_socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM)
                    forward_socket.connect((target_host, CONTROL_PORT))
                    welcome_message = forward_socket.recv(BUFFER_SIZE)
                    relative_path_to_curr_dir = ""
                    parts = str(self.current_dir).split('/')

                    # Assuming the part you want to remove is always the second element (index 1)
                    # and you want to keep everything from the third element onwards (index 2)
                    extracted_parts = parts[2:]

                    extracted_path = "/" + "/".join(extracted_parts)
                    command_to_send = f"FORWARD_RMDIR {os.path.join(extracted_path, dirname)}"
                    forward_socket.send(command_to_send.encode())
                    response = forward_socket.recv(
                        BUFFER_SIZE).decode().strip()
                    if response.startswith("250"):
                        print(
                            f"FORWARD_RMDIR to node {target_id} successful.")
                    else:
                        print(
                            f"FORWARD_RMDIR to node {target_id} failed with response: {response}")
                except Exception as e:
                    print(f"Error forwarding RMDIR to node {target_id}: {e}")
                finally:
                    if forward_socket:
                        forward_socket.close()
        except Exception as e:
            print(f"Error in RMD: {e}")
            client_socket.send(b"550 Failed to delete directory.\r\n")

    def handle_deleg(self, client_socket, filename):
        """
        Handles the DELE command in a decentralized manner.
        Deletes the file from all servers that have a replica.
        """
        print(
            f"DEBUG: Starting handle_dele for filename: {filename}")  # Log start of function

        virtual_current_dir = self.current_dir
        if str(virtual_current_dir)[0] == "/":
            virtual_current_dir = str(virtual_current_dir)[1:]
        # Log virtual current dir
        print(f"DEBUG: Virtual current directory: {virtual_current_dir}")

        file_path = os.path.join(os.path.join(
            self.resources_dir, virtual_current_dir), filename)
        relative_file_path = os.path.join(virtual_current_dir, filename)
        print(f"DEBUG: Constructed file_path: {file_path}")  # Log file_path
        # Log relative_file_path
        print(f"DEBUG: Constructed relative_file_path: {relative_file_path}")

        # Log loop start
        print(f"DEBUG: Iterating through chord nodes to forward DELE command.")
        for node_config in self.chord_nodes_config:
            # Unpack for clarity
            node_host, node_port, node_id = node_config[0], node_config[1], node_config[2]
            if node_id == self.node_id:  # node_config[2] is node_id
                # Log skip self
                print(
                    f"DEBUG: Skipping DELE forwarding to self (node_id: {node_id}).")
                continue

            target_host = node_host
            target_port = node_port
            # Log target node
            print(
                f"DEBUG: Attempting to forward DELE to node {node_id} at {target_host}:{CONTROL_PORT}")

            forward_socket = None  # Initialize outside try block for finally scope
            try:
                forward_socket = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                forward_socket.connect(
                    (target_host, CONTROL_PORT))  # Use CONTROL_PORT
                # Log connection success
                print(
                    f"DEBUG: Connected to node {node_id} at {target_host}:{CONTROL_PORT}")

                welcome_message = forward_socket.recv(
                    BUFFER_SIZE)  # Welcome message
                # Log welcome message
                print(
                    f"DEBUG: Received welcome message from node {node_id}: {welcome_message.decode().strip()}")

                command_to_send = f"FORWARD_DELE {virtual_current_dir}/{filename}\r\n"
                # Log command sent
                print(
                    f"DEBUG: Sending command to node {node_id}: {command_to_send.strip()}")
                forward_socket.send(command_to_send.encode())

                response = forward_socket.recv(
                    BUFFER_SIZE).decode().strip()
                print(
                    f"DEBUG: Response from node {node_id} for FORWARD_DELE: {response}")  # Log response received
                if response.startswith("250"):
                    print(
                        f"DEBUG: FORWARD_DELE to node {node_id} successful.")  # Log successful forward
                else:
                    print(
                        f"DEBUG: FORWARD_DELE to node {node_id} failed with response: {response}")  # Log failed forward
            except Exception as e:
                print(
                    f"ERROR: Error forwarding DELE to node {node_id}: {e}")  # Keep existing error log, but mark as ERROR
            finally:
                if forward_socket:  # Check if socket was created before closing
                    forward_socket.close()
                    # Log socket closure
                    print(f"DEBUG: Closed forward_socket to node {node_id}.")
                else:
                    # Log if socket was not initialized
                    print(
                        f"DEBUG: forward_socket was not initialized, no need to close.")

            # Delete the file from the current server as well
        try:
            # Log local file check
            print(f"DEBUG: Checking if file exists locally: {file_path}")
            if os.path.exists(file_path):
                # Log file found locally
                print(
                    f"DEBUG: File {filename} found locally, proceeding with deletion.")
                os.remove(file_path)
                # Log local deletion success
                print(f"DEBUG: File {filename} deleted locally.")
            else:
                # Log file not found locally
                print(
                    f"DEBUG: File {filename} not found locally, skipping local deletion.")
                # Keep existing user-facing message
                print(f"File {filename} not found locally.")

            client_socket.send(b"250 File deleted successfully.\r\n")
            # Log client response success
            print(f"DEBUG: Sent '250 File deleted successfully.' to client.")

        except Exception as e:
            # Keep existing error log, but mark as ERROR
            print(f"ERROR: Error deleting file locally in DELE: {e}")
            client_socket.send(b"550 Failed to delete file.\r\n")
            # Log client response failure
            print(f"DEBUG: Sent '550 Failed to delete file.' to client.")

    def handle_rmdg(self, client_socket, dirname, virtual_current_dir):
        try:
            if str(virtual_current_dir)[0] == "/":
                virtual_current_dir = str(virtual_current_dir)[1:]
            # logs
            print(
                f"RMD command received for directory: {dirname}, virtual path: {virtual_current_dir}")

            folder_path = os.path.join(os.path.join(
                self.resources_dir, virtual_current_dir), dirname)
            relative_folder_path = os.path.join(virtual_current_dir, dirname)

            # Check if the directory exists locally
            os.rmdir(folder_path)
            # Update folder_replicas for the newly created directory
            if relative_folder_path in self.folder_replicas:  # Use relative_file_path as key
                # Use relative_file_path as key
                replica_nodes = self.folder_replicas[relative_folder_path]
                print(
                    f"Folder {dirname} replicas found at nodes: {replica_nodes}")
                # Iterate over a copy to allow modification
                for node_id in list(replica_nodes):
                    if node_id != self.node_id:
                        target_node_info = None
                        for node_config in self.chord_nodes_config:
                            if node_config[2] == node_id:  # node_config[2] is node_id
                                target_node_info = node_config
                                break
                        if target_node_info:
                            target_host = target_node_info[0]
                            target_port = target_node_info[1]
                            try:
                                forward_socket = socket.socket(
                                    socket.AF_INET, socket.SOCK_STREAM)
                                forward_socket.connect(
                                    (target_host, CONTROL_PORT))  # Use CONTROL_PORT
                                # Welcome
                                # Welcome message
                                forward_socket.recv(BUFFER_SIZE)
                                forward_socket.send(
                                    f"FORWARD_RMDIR {virtual_current_dir}/{dirname}\r\n".encode())
                                response = forward_socket.recv(
                                    BUFFER_SIZE).decode().strip()
                                print(
                                    f"Response from node {node_id} for FORWARD_RMDIR: {response}")
                                if response.startswith("250"):
                                    pass  # Successfully forwarded deletion
                                else:
                                    print(
                                        f"FORWARD_RMDIR to node {node_id} failed.")
                            except Exception as e:
                                print(
                                    f"Error forwarding RMDIR to node {node_id}: {e}")
                            finally:
                                forward_socket.close()

                # Delete the file from the current server as well

                    # Update file_replicas dictionary
                if relative_folder_path in self.folder_replicas:  # Use relative_file_path as key
                    del self.folder_replicas[relative_folder_path]
                    self.broadcast_folder_replicas_delete(relative_folder_path)

            print(f"  Relative new directory path: {relative_folder_path}")
            client_socket.send(b"250 Directory deleted successfully.\r\n")
        except Exception as e:
            print(f"Error deleting folder locally in RMDIR: {e}")
            client_socket.send(b"550 Failed to delete folder.\r\n")
    def handle_auth(self, client_socket, arg):
        client_socket.send(b"AUTH not implemented yet.\r\n")

    def handle_opts(self, client_socket, arg):
        if arg.upper() == "UTF8 ON":
            self.utf8_mode = True  # Enable UTF8 mode
            client_socket.send(b"200 UTF8 mode enabled.\r\n")
        else:
            client_socket.send(b"502 Command: OPTS not implemented.\r\n")

    def handle_user(self, client_socket, username):
        return b"331 User name okay, need password.\r\n"
    
    def handle_rnfr(self, client_socket, filename):
        if os.path.exists(os.path.join(self.current_dir, filename)):
            client_socket.send(
                b"350 File exists, ready for destination name.\r\n")
            return filename
        else:
            client_socket.send(b"550 File not found.\r\n")
            return None

    def handle_rnto(self, client_socket, rename_from, rename_to):
        if rename_from:
            try:
                os.rename(os.path.join(self.current_dir, rename_from),
                          os.path.join(self.current_dir, rename_to))
                client_socket.send(b"250 File renamed successfully.\r\n")
            except Exception as e:
                print(f"Error in RNTO: {e}")
                client_socket.send(b"550 Failed to rename file.\r\n")
        else:
            client_socket.send(b"503 Bad sequence of commands.\r\n")

    def handle_mkd(self, client_socket, dirname):
        try:
            os.mkdir(os.path.join(self.current_dir, dirname))
            client_socket.send(
                f'257 "{dirname}" directory created\r\n'.encode())
        except Exception as e:
            client_socket.send(b"550 Failed to create directory.\r\n")

    def handle_retr(self, client_socket, data_socket, filename):
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return

        try:
            client_socket.send(b"150 Opening data connection.\r\n")
            conn, addr = data_socket.accept()
            with open(os.path.join(self.current_dir, filename), 'rb') as file:
                while True:
                    data = file.read(BUFFER_SIZE)
                    if not data:
                        break
                    conn.send(data)
            conn.close()
            client_socket.send(b"226 Transfer complete.\r\n")
        except Exception as e:
            print(f"Error in RETR: {e}")
            client_socket.send(b"550 Failed to retrieve file.\r\n")
        finally:
            data_socket.close()
            
    def handle_list(self, client_socket, data_socket):
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return

        client_socket.send(b"150 Here comes the directory listing.\r\n")

        try:
            conn, addr = data_socket.accept()  # Accept incoming connection from client
            # List all entries in current directory
            entries = os.listdir(self.current_dir)
            for entry in entries:
                full_path = os.path.join(self.current_dir, entry)
                if os.path.isdir(full_path):
                    # Format for directories
                    file_info = f'drwxr-xr-x 1 owner group {os.stat(full_path).st_size} {time.strftime("%b %d %H:%M", time.gmtime(os.stat(full_path).st_mtime))} {entry}\r\n'
                else:
                    # Format for files
                    file_info = f'-rw-r--r-- 1 owner group {os.stat(full_path).st_size} {time.strftime("%b %d %H:%M", time.gmtime(os.stat(full_path).st_mtime))} {entry}\r\n'

                conn.send(file_info.encode())  # Send each entry info to client

            conn.close()  # Close data connection after sending all entries
            # Send completion response
            client_socket.send(b"226 Directory send OK.\r\n")
        except Exception as e:
            print(f"Error in LIST: {e}")
            client_socket.send(b"425 Can't open data connection.\r\n")
        finally:
            data_socket.close()  # Ensure data socket is closed

    def handle_stor(self, client_socket, data_socket, filename):
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return

        try:
            client_socket.send(b"150 Ok to send data.\r\n")
            conn, addr = data_socket.accept()
            with open(os.path.join(self.current_dir, filename), 'wb') as file:
                while True:
                    data = conn.recv(BUFFER_SIZE)
                    if not data:
                        break
                    file.write(data)
            conn.close()
            client_socket.send(b"226 Transfer complete.\r\n")
        except Exception as e:
            print(f"Error in STOR: {e}")
            client_socket.send(b"550 Failed to store file.\r\n")
        finally:
            data_socket.close()
            
    def handle_pwd(self, client_socket):
        response = f'257 "{self.current_dir}"\r\n'
        client_socket.send(response.encode())

    def handle_cwd(self, client_socket, path):
        try:
            # Check if the path is absolute or relative
            if not os.path.isabs(path):
                path = os.path.join(self.current_dir, path)

            print(f"Changing directory to {path}")
            os.chdir(path)
            self.current_dir = os.getcwd()
            client_socket.send(b"250 Directory successfully changed.\r\n")
            print(f"Current directory: {self.current_dir}")
        except Exception as e:
            print(f"Error in CWD: {e}")
            client_socket.send(b"550 Failed to change directory.\r\n")
    
    def handle_mdtm(self, client_socket, filename):
        try:
            mtime = os.path.getmtime(os.path.join(
                self.current_dir, filename))
            mtimestr = time.strftime("%Y%m%d%H%M%S", time.gmtime(mtime))
            client_socket.send(f"213 {mtimestr}\r\n".encode())
        except Exception as e:
            client_socket.send(
                b"550 Could not get file modification time.\r\n")
    
    def handle_size(self, client_socket, filename):
        try:
            size = os.path.getsize(os.path.join(
                self.current_dir, filename))
            client_socket.send(f"213 {size}\r\n".encode())
        except Exception as e:
            client_socket.send(b"550 Could not get file size.\r\n")

    def handle_pasv(self, client_socket):
        data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_socket.bind((self.host, 0))
        data_socket.listen(1)
        ip, port = data_socket.getsockname()
        ip_parts = ip.split('.')
        port_hi = port // 256
        port_lo = port % 256
        response = f'227 Entering Passive Mode ({",".join(ip_parts)},{port_hi},{port_lo})\r\n'
        client_socket.send(response.encode())
        return data_socket
    
    def start(self):
        while True:
            client_sock, addr = self.server_socket.accept()
            print(f"Connection from {addr}")
            threading.Thread(target=self.handle_client,
                             args=(client_sock,addr)).start()

    


if __name__ == "__main__":
    if len(sys.argv) < 3:  # Expecting node_id, config_string, host_ip
        print("""Read txt""")
        sys.exit(1)

    node_id = int(sys.argv[1])
    host_ip = sys.argv[2]
    print(f"host_ip from command line: {host_ip}")

    ftp_server = FTPServer(host=host_ip, node_id=node_id)  # Pass string
    ftp_server.start()
