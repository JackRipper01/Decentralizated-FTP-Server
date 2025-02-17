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
HELLO_PORT = 3000  # Port for UDP global server communication
FILE_REPLICAS_PORT = 3001  # Port for UDP file replicas communication
FOLDER_REPLICAS_PORT = 3002  # Port for UDP folder replicas communication
NODE_DISCOVERY_INTERVAL = 1  # Interval in seconds for broadcasting hello messages
TEMP_DOWNLOAD_DIR_NAME = "temp_downloads"
# Define timeout, e.g., 3 times the broadcast interval
INACTIVE_NODE_TIMEOUT = 10  # Timeout in seconds for removing inactive nodes
REPLICATION_FACTOR = 3


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

        if not self.resources_dir.exists():
            self.resources_dir.mkdir()
            print(f"Created directory: {self.resources_dir}")
        else:
            print(f"Directory already exists: {self.resources_dir}")
        self.current_dir = self.resources_dir
        print(self.current_dir)

        self.discovery_timer = None  # To hold the timer object
        
        self.node_id = node_id  # Unique ID for this Chord node
        self.chord_nodes_config = []  # Initialize as empty list
        self_config = [self.host, CONTROL_PORT,
                       self.node_id, time.time()]  # timestamp
        self.chord_nodes_config.append(self_config)
        # Dictionary to track folder replicas: {Key: Full Path of Folder , Value: [node_id1, node_id2, node_id3]}
        self.folder_replicas = {}
        self.start_listeners()
        self.start_node_discovery_broadcast()

        # Dictionary to track file replicas: {Key: Full Path of File including filename , Value: [node_id1, node_id2, node_id3]}
        self.file_replicas = {}
        self.update_file_replicas_on_startup()
        # Broadcast the updated file_replicas to other nodes
        self.broadcast_file_replicas_merge()

        
        self.update_folder_replicas_on_startup()
        # Broadcast the updated file_replicas to other nodes
        self.broadcast_folder_replicas_merge()

        # Log it
        print(f"Node {self.node_id} initialized with self config: {self_config}")

        self.start_inactive_node_removal_thread()
        
        

    # region COMMS

    def update_file_replicas_on_startup(self):
        """
        Scans the server's resource directory and all subdirectories,
        updating the file_replicas dictionary for all files found.
        This is intended to be run on server startup.
        """
        print("Updating file replicas on startup (recursive scan)...")
        # os.walk returns (root, directories, files) for each directory
        for root, _, files in os.walk(self.resources_dir):
            # Iterate through files in the current directory (root)
            for filename in files:
                # Construct full file path
                file_path = os.path.join(root, filename)
                # Construct relative file path
                relative_file_path = os.path.relpath(
                    file_path, self.resources_dir)

                # Now process each file as before, but use relative_file_path as key
                if relative_file_path not in self.file_replicas:
                    self.file_replicas[relative_file_path] = [self.node_id]
                    print(
                        f"Registered file '{relative_file_path}' in file_replicas for node {self.node_id}")
                elif self.node_id not in self.file_replicas[relative_file_path]:
                    self.file_replicas[relative_file_path].append(self.node_id)
                    print(
                        f"Added node {self.node_id} to replicas for existing file '{relative_file_path}'")
                else:
                    print(
                        f"File '{relative_file_path}' already registered for node {self.node_id} in file_replicas.")
        print("Recursive file replicas update on startup complete.")
        print(f"Current file_replicas: {self.file_replicas}")

    def update_folder_replicas_on_startup(self):
        """
        Scans the server's resource directory and all subdirectories,
        updating the folder_replicas dictionary for all folders found.
        This is intended to be run on server startup.
        """
        print("Updating folder replicas on startup (recursive scan)...")
        # os.walk returns (root, directories, files) for each directory
        for root, directories, _ in os.walk(self.resources_dir):
            # Iterate through directories in the current directory (root)
            for directory in directories:
                # Construct full folder path
                folder_path = os.path.join(root, directory)
                # Construct relative folder path
                relative_folder_path = os.path.relpath(
                    folder_path, self.resources_dir)

                # Now process each folder as before, but use relative_folder_path as key
                if relative_folder_path not in self.folder_replicas:
                    self.folder_replicas[relative_folder_path] = [self.node_id]
                    print(
                        f"Registered folder '{relative_folder_path}' in folder_replicas for node {self.node_id}")
                elif self.node_id not in self.folder_replicas[relative_folder_path]:
                    self.folder_replicas[relative_folder_path].append(
                        self.node_id)
                    print(
                        f"Added node {self.node_id} to replicas for existing folder '{relative_folder_path}'")
                else:
                    print(
                        f"Folder '{relative_folder_path}' already registered for node {self.node_id} in folder_replicas.")
        print("Recursive folder replicas update on startup complete.")
        print(f"Current folder_replicas: {self.folder_replicas}")

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
            target=self.listen_for_file_replicas_merge, daemon=True).start()
        threading.Thread(
            target=self.listen_for_folder_replicas_merge, daemon=True).start()
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
            time_random= random.uniform(0.1, NODE_DISCOVERY_INTERVAL)
            time.sleep(time_random)  # Broadcast every N seconds

    def broadcast_file_replicas_merge(self):
        """Broadcasts a UDP message to add node ids to all other file_replicas of other servers."""
        broadcast_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_socket.bind(('', 0))  # Bind to any available port

        message = self.serialize_file_replicas()  # Serialize file_replicas to string
        message = f"FILE_REPLICAS_MERGE:{message}".encode(
            'utf-8')  # Add a prefix

        for _ in range(2):  # Send multiple times for reliability in UDP
            broadcast_socket.sendto(
                message, ('<broadcast>', FILE_REPLICAS_PORT))
            time_to_sleep = random.uniform(0.1, 2)
            time.sleep(time_to_sleep)  # small delay between broadcasts

        broadcast_socket.close()
        print(f"Node {self.node_id}: Broadcasted file_replicas merge.") 

    def broadcast_folder_replicas_merge(self):
        """Broadcasts a UDP message to add node ids to all other folder_replicas of other servers."""
        broadcast_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_socket.bind(('', 0))

        # Serialize folder_replicas to string
        message = self.serialize_folder_replicas()
        message = f"FOLDER_REPLICAS_MERGE:{message}".encode(
            'utf-8')

        for _ in range(2):  # Send multiple times for reliability in UDP
            broadcast_socket.sendto(
                message, ('<broadcast>', FOLDER_REPLICAS_PORT))
            time_to_sleep = random.uniform(0.1, 2)
            time.sleep(time_to_sleep)

        broadcast_socket.close()
        print(f"Node {self.node_id}: Broadcasted folder_replicas merge.")
        
    def broadcast_file_replicas_delete(self, file_path):
        """Broadcasts a UDP message to delete node ids from all other file_replicas of other servers."""
        broadcast_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_socket.bind(('', 0))  # Bind to any available port

        message_payload = file_path  # the file_path to be deleted from replicas
        message = f"FILE_REPLICAS_DELETE:{message_payload}".encode(
            'utf-8')  # Add a delete prefix

        for _ in range(2):  # Send multiple times for reliability in UDP
            broadcast_socket.sendto(
                message, ('<broadcast>', FILE_REPLICAS_PORT))
            time_to_sleep = random.uniform(0.1, 2)
            time.sleep(time_to_sleep)  # small delay between broadcasts

        broadcast_socket.close()
        print(
            f"Node {self.node_id}: Broadcasted file_replicas delete for file: {file_path}")

    def broadcast_folder_replicas_delete(self, folder_path):
        """Broadcasts a UDP message to delete node ids from all other folder_replicas of other servers."""
        broadcast_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_socket.bind(('', 0))

        message_payload = folder_path  # the folder_path to be deleted from replicas
        message = f"FOLDER_REPLICAS_DELETE:{message_payload}".encode(
            'utf-8')

        for _ in range(2):  # Send multiple times for reliability in UDP
            broadcast_socket.sendto(
                message, ('<broadcast>', FOLDER_REPLICAS_PORT))
            # sleep randomly between 0.1 and 2 seconds
            time_to_sleep = random.uniform(0.1, 2)
            time.sleep(time_to_sleep)

        broadcast_socket.close()
        print(
            f"Node {self.node_id}: Broadcasted folder_replicas delete for folder: {folder_path}")

    def reset_discovery_timer(self):
        """Resets the discovery timer. Cancels any existing timer and starts a new one."""
        if self.discovery_timer:
            self.discovery_timer.cancel()  # Cancel the existing timer if running

        self.discovery_timer = threading.Timer(8.0, self.redistribute_replicas) # Create a new timer
        self.discovery_timer.start() # Start the new timer
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
                                # Reset and start the timer every time a new node is discovered
                                # self.reset_discovery_timer()
                                self.broadcast_file_replicas_merge()
                                self.broadcast_folder_replicas_merge()
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

    def listen_for_file_replicas_merge(self):
        """Listens for UDP 'file replicas merge' messages from other nodes and updates file replicas."""
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_address = ("", FILE_REPLICAS_PORT)
        listen_socket.bind(listen_address)

        while True:
            try:
                data, address = listen_socket.recvfrom(BUFFER_SIZE)
                ip, port = address  # ip and self.host has format x.x.x.x
                if ip == self.host:
                    continue
                message = data.decode().strip()
                if message.startswith("FILE_REPLICAS_MERGE:"):
                    replicas_str = message[len("FILE_REPLICAS_MERGE:"):]
                    updated_replicas = self.deserialize_file_replicas(
                        replicas_str)
                    if updated_replicas:
                        for file_path, updated_node_ids in updated_replicas.items():
                            if file_path in self.file_replicas:
                                existing_node_ids = set(
                                    self.file_replicas[file_path])
                                new_node_ids = set(updated_node_ids)
                                merged_node_ids = list(
                                    existing_node_ids.union(new_node_ids))
                                self.file_replicas[file_path] = merged_node_ids
                            else:
                                self.file_replicas[file_path] = updated_node_ids
                        print(
                            f"Node {self.node_id}: Received and merged file_replicas update from {address}.")
                        print("Current file_replicas:", self.file_replicas)

            except Exception as e:
                print(
                    f"Error listening for file replicas merge messages on Node {self.node_id}: {e}")

    def listen_for_folder_replicas_merge(self):
        """Listens for UDP 'folder replicas merge' messages from other nodes and updates folder replicas."""
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_address = ("", FOLDER_REPLICAS_PORT)
        listen_socket.bind(listen_address)

        while True:
            try:
                data, address = listen_socket.recvfrom(BUFFER_SIZE)
                ip, port = address  # ip and self.host has format x.x.x.x
                if ip == self.host:
                    continue
                message = data.decode().strip()
                if message.startswith("FOLDER_REPLICAS_MERGE:"):
                    replicas_str = message[len("FOLDER_REPLICAS_MERGE:"):]
                    updated_replicas = self.deserialize_folder_replicas(
                        replicas_str)
                    if updated_replicas:
                        for folder_path, updated_node_ids in updated_replicas.items():
                            if folder_path in self.folder_replicas:
                                existing_node_ids = set(
                                    self.folder_replicas[folder_path])
                                new_node_ids = set(updated_node_ids)
                                merged_node_ids = list(
                                    existing_node_ids.union(new_node_ids))
                                self.folder_replicas[folder_path] = merged_node_ids
                            else:
                                self.folder_replicas[folder_path] = updated_node_ids
                        print(
                            f"Node {self.node_id}: Received and merged folder_replicas update from {address}.")
                        print("Current folder_replicas:", self.folder_replicas)

            except Exception as e:
                print(
                    f"Error listening for folder replicas merge messages on Node {self.node_id}: {e}")

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
            for node in removed_nodes:
                self.chord_nodes_config.remove(node)
                # remove node id from file_replicas and folder_replicas
                node_id = node[2]
                self.remove_node_from_file_replicas(node_id)
                self.remove_node_from_folder_replicas(node_id)
            print(f"Removed inactive nodes: {removed_nodes}")

            print(
                f"Updated chord_nodes_config after remove inactive nodes: {self.chord_nodes_config}")
            print(f"Current file_replicas: {self.file_replicas}")
            print(f"Current folder_replicas: {self.folder_replicas}")
            self.redistribute_replicas(removed_nodes)

    def remove_node_from_file_replicas(self, node_id):
        """Removes a node id from all file_replicas entries."""
        for file_path, node_ids in list(self.file_replicas.items()):
            if node_id in node_ids:
                node_ids.remove(node_id)
                if not node_ids:
                    del self.file_replicas[file_path]
                else:
                    self.file_replicas[file_path] = node_ids
                print(
                    f"Removed node {node_id} from file_replicas for file '{file_path}'")
    
    def remove_node_from_folder_replicas(self, node_id):
        """Removes a node id from all folder_replicas entries."""
        for folder_path, node_ids in list(self.folder_replicas.items()):
            if node_id in node_ids:
                node_ids.remove(node_id)
                if not node_ids:
                    del self.folder_replicas[folder_path]
                else:
                    self.folder_replicas[folder_path] = node_ids
                print(
                    f"Removed node {node_id} from folder_replicas for folder '{folder_path}'")
    
    def redistribute_replicas(self, removed_nodes=[]):
        """Redistributes replicas that were on removed nodes."""
        #create set
        
        for i in range(3):
            worked=set()
            if self.chord_nodes_config==1:
                print("Nothing to do,I am alone T_T")
                return
            print("entered redistribute_replicas")
            print(
                f"Node {self.node_id}: 5 seconds elapsed without new node discovery. Calling MyFunction()")
            removed_node_ids = {node[2] for node in removed_nodes}

            # Folder replicas redistribution (similar logic as files)
            for folder_path, node_ids in list(self.folder_replicas.items()):
                needs_redistribution = False
                updated_node_ids = [
                    nid for nid in node_ids if nid not in removed_node_ids]
                if len(updated_node_ids) < REPLICATION_FACTOR and len(self.chord_nodes_config) >= REPLICATION_FACTOR-1:
                    needs_redistribution = True

                if needs_redistribution:
                    print(f"Folder {folder_path} needs replica redistribution.")
                    target_nodes = self.find_successors_for_replication(
                        folder_path, is_folder=True, exclude_nodes=updated_node_ids)
                    final_replica_nodes = list(
                        set(updated_node_ids + target_nodes))[:REPLICATION_FACTOR]

                    self.folder_replicas[folder_path] = final_replica_nodes
                    print(
                        f"Updated replicas for {folder_path} to: {final_replica_nodes}")
                    # Replication to new nodes would be triggered here.
                    for node_id in target_nodes:
                        if node_id not in updated_node_ids and node_id == self.node_id:
                            self.create_folder(folder_path)

                print("Current folder_replicas:", self.folder_replicas)
            self.discovery_timer = None  # Reset the timer after redistribution

            # File replicas redistribution
            # Iterate over a copy to allow modification
            for file_path, node_ids in list(self.file_replicas.items()):
                needs_redistribution = False
                updated_node_ids = [
                    nid for nid in node_ids if nid not in removed_node_ids]
                if len(updated_node_ids) < REPLICATION_FACTOR and len(self.chord_nodes_config) >= REPLICATION_FACTOR-1:
                    needs_redistribution = True

                if needs_redistribution:
                    if len(list(updated_node_ids)) ==2:
                        print(f"File {file_path} needs replica redistribution.")
                        successor_list = self.find_successors(
                            updated_node_ids[0], self.chord_nodes_config, num_successors=REPLICATION_FACTOR)
                        if successor_list[1][2] in updated_node_ids:
                            print(
                                "THE SECOND NODE OF THE SUCCESSOR LIST IS IN UPDATE_NODE_IDS ,GOOD")
                        if len(successor_list) >= 3:
                            source_node_info = successor_list[0]
                            if successor_list[2][2] not in updated_node_ids:
                                target_node_info = successor_list[2]
                            elif successor_list[1][2] not in updated_node_ids:
                                target_node_info = successor_list[1]
                                print(f"sucessor of {updated_node_ids[0]} have the file {file_path} ,copying to {target_node_info[2]} WTF?!")
                            else:
                                print(f"Something is wrong cause was needed replication and all successors of node {updated_node_ids[0]} already have the file {file_path}")
                                raise Exception(
                                    "Something is wrong cause was needed replication and all successor of node {updated_node_ids[0]} already have the file")
                        else:
                            source_node_info = successor_list[0]
                            target_node_info = successor_list[1]
                        

                        final_replica_nodes = []
                        for node_info in successor_list:
                            node_host, node_control_port, node_id, node_time = node_info
                            final_replica_nodes.append(node_id)
                        print("final_replica_nodes", final_replica_nodes)
                        self.file_replicas[file_path] = final_replica_nodes
                        print(
                            f"Updated replicas for {file_path} to: {final_replica_nodes}")
                        
                        for node_id in updated_node_ids:
                            if node_id == self.node_id and node_id == source_node_info[2]:
                                worked.add(self.copy_file_to_node(file_path, target_node_info))
                                
                    elif len(list(updated_node_ids)) == 1:
                        print(f"File {file_path} needs replica redistribution.")
                        successor_list = self.find_successors(updated_node_ids[0], self.chord_nodes_config, num_successors=REPLICATION_FACTOR)
                        if successor_list[0][2] == updated_node_ids[0]:
                            print("THE FIRST NODE OF THE SUCCESSOR LIST IS EQUAL TO UPDATE_NODE_IDS[0]")
                        if len(successor_list)>=3:
                            source_node_info = successor_list[0]
                            target_one_node_info = successor_list[1]
                            target_two_node_info = successor_list[2]
                        else:
                            source_node_info = successor_list[0]
                            target_one_node_info = successor_list[1]

                        final_replica_nodes=[]
                        for node_info in successor_list:
                            node_host, node_control_port, node_id, node_time = node_info 
                            final_replica_nodes.append(node_id)
                        print("final_replica_nodes", final_replica_nodes)
                        self.file_replicas[file_path] = final_replica_nodes
                        print(
                            f"Updated replicas for {file_path} to: {final_replica_nodes}")
                        
                        for node_id in updated_node_ids:
                            if node_id == self.node_id and node_id == source_node_info[2]:
                                worked.add(self.copy_file_to_node(file_path, target_one_node_info))
                                if len(successor_list)>=3:
                                    worked.add(self.copy_file_to_node(file_path, target_two_node_info))
                    else:
                        print("Something is wrong cause was needed replication and update_node_ids is not 1 or 2")

                print("Current file_replicas:", self.file_replicas)
            if not (False in worked):
                break


    def copy_file_to_node(self, file_path, node_info):
        """Creates a folder in the specified node."""
        print(f"NODE {self.node_id} : COPING {file_path} to node {node_info[2]} .")
        # self.chord_nodes_config = []  # Initialize as empty list self_config = [self.host, CONTROL_PORT, self.node_id, time.time()] self.chord_nodes_config.append(self_config)

        node_ip, node_control_port, node_id, node_time = node_info # Unpack node_info
        

        ftp_client_socket = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
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
        print(f"Node {self.node_id}: Data server IP: {data_server_ip}, port: {data_server_port}")
        ftp_data_socket_client = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM)
        ftp_data_socket_client.connect(
            (data_server_ip, data_server_port))

        ftp_client_socket.send(f"COPY {file_path}\r\n".encode())
        full_file_path= os.path.join(self.resources_dir, file_path)
        print("full_file_path", full_file_path)
        with open(full_file_path, 'rb') as temp_file_forward:
            while True:
                data_to_forward = temp_file_forward.read(
                    BUFFER_SIZE)
                if not data_to_forward:
                    break
                ftp_data_socket_client.sendall(
                    data_to_forward)

        ftp_data_socket_client.close()
        response = ftp_client_socket.recv(
            BUFFER_SIZE).decode()  # Get 226 or error
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
        
        #get filename from file_path example: file_path = "candelasa/mongo.txt" and filename should be  mongo.txt , other example file_path = "loco.mp4" and filename should be "loco.mp4"
              
        key=self.get_key(file_path)
        temp_file_path = temp_dir_path.joinpath(f"temp_{key}")
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
            

        
    def serialize_file_replicas(self):
        """Serializes the file_replicas dictionary to a string format (filename:node_id1,node_id2,...;...)."""
        serialized_str = ""
        for file_path, node_ids in self.file_replicas.items():  # file_path is now relative
            serialized_str += f"{file_path}:{','.join(map(str, node_ids))};"
        return serialized_str.rstrip(';')  # Remove trailing semicolon if any

    def serialize_folder_replicas(self):
        """Serializes the folder_replicas dictionary to a string format (folder_path:node_id1,node_id2,...;...)."""
        serialized_str = ""
        for folder_path, node_ids in self.folder_replicas.items():  # folder_path is now relative
            serialized_str += f"{folder_path}:{','.join(map(str, node_ids))};"
        return serialized_str.rstrip(';')  # Remove trailing semicolon if any

    def deserialize_file_replicas(self, replicas_str):
        """Deserializes the string representation back to a file_replicas dictionary."""
        replicas_dict = {}
        if not replicas_str:  # Handle empty string
            return replicas_dict

        for item in replicas_str.split(';'):
            parts = item.split(':', 1)  # Split only once at the first ':'
            if len(parts) == 2:  # Ensure it has both filename and node_ids
                file_path = parts[0]  # file_path is now relative
                node_ids_str = parts[1]
                node_ids = [int(node_id) for node_id in node_ids_str.split(
                    ',')] if node_ids_str else []  # Handle empty node_ids string
                # file_path is now relative
                replicas_dict[file_path] = node_ids
        return replicas_dict

    def deserialize_folder_replicas(self, replicas_str):
        """Deserializes the string representation back to a folder_replicas dictionary."""
        replicas_dict = {}
        if not replicas_str:  # Handle empty string
            return replicas_dict

        for item in replicas_str.split(';'):
            parts = item.split(':', 1)
            if len(parts) == 2:
                folder_path = parts[0]  # folder_path is now relative
                node_ids_str = parts[1]
                node_ids = [int(node_id) for node_id in node_ids_str.split(
                    ',')] if node_ids_str else []

                replicas_dict[folder_path] = node_ids
        return replicas_dict

# endregion COMMS

# Chord methods (No changes needed in Chord logic itself)

    def get_key(self, filename):
        """
        Generates a Chord key for a given filename using SHA1 hash.
        Now generates keys in the range 0 to 2047.
        """
        return int(hashlib.sha1(filename.encode()).hexdigest(), 16) % 2048

    def find_successors(self, key, nodes_config, num_successors=3):
        """
        Finds a list of successor nodes for a given key in the Chord ring,
        including the node itself and the next ones in the ring.
        Ensures no duplicate nodes are returned in the list.

        Args:
            key (int): The key to find successors for.
            nodes_config (list): List of node configurations.
            num_successors (int): The number of successors to return (default is 3).

        Returns:
            list: A list of unique node info lists, representing the successors.
                  The list will contain at most num_successors unique nodes,
                  and might be shorter if there are fewer unique nodes in the ring.
        """
        ring_size = 1024  # Same as key space
        # Sort nodes by node_id
        sorted_nodes = sorted(nodes_config, key=lambda node_info: node_info[2])
        if not sorted_nodes:
            # If no other nodes, current node is the only one (return self num_successors times - but only unique once)
            # Return only self once as it's the only unique node
            return [[self.host, CONTROL_PORT, self.node_id]]

        primary_successor = None
        successor_index = -1  # Keep track of the index of the primary successor

        for i in range(len(sorted_nodes)):
            current_node_info = sorted_nodes[i]
            next_index = (i + 1) % len(sorted_nodes)
            next_node_info = sorted_nodes[next_index]

            current_node_id = current_node_info[2]
            next_node_id = next_node_info[2]

            if current_node_id < next_node_id:  # Normal case, ring not wrapped around
                if current_node_id < key <= next_node_id:
                    primary_successor = next_node_info
                    successor_index = next_index
                    break
            # Ring wrapped around, e.g., node IDs are [800, 900, 100, 200]
            else:
                if key > current_node_id or key <= next_node_id:  # Key is in the wrap-around range
                    primary_successor = next_node_info
                    successor_index = next_index
                    break

        if not primary_successor:
            # If key is smaller than the smallest node ID, the first node in the ring is the successor
            primary_successor = sorted_nodes[0]
            successor_index = 0

        successors_list = []
        added_node_ids = set()  # Keep track of added node IDs to avoid duplicates

        # Add the primary successor if it's not already added
        if primary_successor[2] not in added_node_ids:
            successors_list.append(primary_successor)
            added_node_ids.add(primary_successor[2])

        # Add the next num_successors - 1 successors, ensuring no duplicates
        for i in range(1, num_successors):
            if len(successors_list) >= len(sorted_nodes):  # Stop if we've added all unique nodes
                break
            next_successor_index = (successor_index + i) % len(sorted_nodes)
            next_successor = sorted_nodes[next_successor_index]
            # Check for duplicates before adding
            if next_successor[2] not in added_node_ids:
                successors_list.append(next_successor)
                added_node_ids.add(next_successor[2])

        return successors_list

    def find_successors_for_replication(self, name, num_successors=REPLICATION_FACTOR, is_folder=False, exclude_nodes=None):
        """
        Finds successor nodes for replication, excluding the current node and optionally other nodes.
        Simple round-robin approach for successor selection based on node IDs.
        """
        print("entered find_successors_for_replication")
        if exclude_nodes is None:
            exclude_nodes = []
        key = self.get_key(name)
        available_nodes = [
            node for node in self.chord_nodes_config if node[2] not in exclude_nodes]
        if not available_nodes:
            return []
        print("available_nodes ", available_nodes)
        sorted_nodes = sorted(
            available_nodes, key=lambda node: node[2])  # Sort by node ID
        num_nodes = len(sorted_nodes)
        if num_nodes < num_successors:
            num_successors = num_nodes  # Adjust if not enough nodes

        start_index = -1
        for i in range(num_nodes):
            # Find the first node with a higher ID
            if sorted_nodes[i][2] > self.node_id:
                start_index = i
                break
        if start_index == -1:  # If no node with higher ID, start from the beginning
            start_index = 0

        successor_nodes_ids = []
        for i in range(num_successors):
            index = (start_index + i) % num_nodes
            # Append only the node ID
            successor_nodes_ids.append(sorted_nodes[index][2])

        return successor_nodes_ids

# Decentralized FTP server methods

    def decentralized_handle_stor(self, client_socket, data_socket, filename, virtual_current_dir,is_forwarded_request=False,):
        """
        Handles the STOR command in a decentralized manner.
        Data is first stored in a temp file, then distributed to responsible nodes.
        """
        #update virtual_current_dir to the same but without the first character
        if str(virtual_current_dir)[0]=="/":
            virtual_current_dir= str(virtual_current_dir)[1:]
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return
        
        file_path = os.path.join(os.path.join(
            self.resources_dir, virtual_current_dir), filename)
        relative_file_path = os.path.join(virtual_current_dir, filename)
        print(f"file_path: {file_path}")
        print(f"relative_file_path: {relative_file_path}")
        key = self.get_key(relative_file_path)
        responsible_nodes_info = self.find_successors(
            key, self.chord_nodes_config, num_successors=3)

        # Ensure temp download directory exists
        temp_dir_path = self.resources_dir.joinpath(
            TEMP_DOWNLOAD_DIR_NAME)
        if not temp_dir_path.exists():
            temp_dir_path.mkdir(parents=True, exist_ok=True)

        # Unique temp file_path could be improved
        temp_file_path = temp_dir_path.joinpath(f"temp_file{key}")
        print(
            f"Node {self.node_id}: Storing data to temp file: {temp_file_path}")

        try:
            client_socket.send(
                b"150 Ok to send data, storing to temp file.\r\n")
            conn, addr = data_socket.accept()
            with open(temp_file_path, 'wb') as temp_file:
                while True:
                    data = conn.recv(BUFFER_SIZE)
                    if not data:
                        break
                    temp_file.write(data)
            conn.close()
            print(f"Node {self.node_id}: Finished receiving data to temp file.")

            if not is_forwarded_request:
                responsible_node_ids = []  # To store node_ids that are responsible for replicas
                # Distribute to responsible nodes
                for node_info in responsible_nodes_info:
                    node_host, node_control_port, node_id, node_time = node_info
                    if node_host == self.host and node_control_port == CONTROL_PORT:
                        # Current node is responsible, store locally (copy from temp)
                        print(
                            f"Node {self.node_id}: Node is responsible, storing locally from temp file.")
                        try:
                            final_file_path = file_path
                            # Use shutil.copy2 for efficient local copy
                            shutil.copy2(temp_file_path, final_file_path)
                            print(
                                f"Node {self.node_id}: Successfully stored locally from temp file: {file_path}")
                            # Only add if local storage was successful
                            if os.path.exists(final_file_path):
                                responsible_node_ids.append(
                                    node_id)  # Add own node_id
                                print("Responsible node_ids:",
                                      responsible_node_ids)

                        except Exception as e:
                            print(
                                f"Node {self.node_id}: Error storing locally from temp file: {e}")
                            # Inform client of local store failure? - consider
                            client_socket.send(
                                b"550 Failed to store file locally from temp.\r\n")

                    else:
                        # Another node is responsible, forward via simplified FTP from temp file
                        print(
                            f"Node {self.node_id}: Forwarding file to node {node_id} from temp file.")
                        try:
                            # Simplified FTP Client Logic (no USER/PASS) - adapted to send from file
                            ftp_client_socket = socket.socket(
                                socket.AF_INET, socket.SOCK_STREAM)
                            ftp_client_socket.connect(
                                (node_host, node_control_port))
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

                            ftp_data_socket_client = socket.socket(
                                socket.AF_INET, socket.SOCK_STREAM)
                            ftp_data_socket_client.connect(
                                (data_server_ip, data_server_port))

                            ftp_client_socket.send(
                                f"FORWARD_STOR {relative_file_path}\r\n".encode())
                            ftp_client_socket.recv(BUFFER_SIZE)  # 150 response

                            with open(temp_file_path, 'rb') as temp_file_forward:
                                while True:
                                    data_to_forward = temp_file_forward.read(
                                        BUFFER_SIZE)
                                    if not data_to_forward:
                                        break
                                    ftp_data_socket_client.sendall(
                                        data_to_forward)

                            ftp_data_socket_client.close()
                            response = ftp_client_socket.recv(
                                BUFFER_SIZE).decode()  # Get 226 or error
                            ftp_client_socket.close()

                            if response.startswith("226"):
                                print(
                                    f"Node {self.node_id}: Successfully forwarded file to node {node_id}.")
                                responsible_node_ids.append(
                                    node_id)  # Add forwarded node_id
                                print("Responsible node_ids:",
                                      responsible_node_ids)
                            else:
                                print(
                                    f"Node {self.node_id}: Failed to forward file to node {node_id}: {response.strip()}")

                        except Exception as e:
                            print(
                                f"Node {self.node_id}: Error forwarding file to node {node_id} via simplified FTP: {e}")

                # Update file_replicas dictionary AFTER all storage attempts
                if responsible_node_ids:  # Only update if there were successful replicas
                    final_file_path =file_path
                    # relative_file_path = os.path.relpath(
                    #     final_file_path, self.resources_dir)  # Get relative path here

                    # Use relative path as key
                    self.file_replicas[relative_file_path] = responsible_node_ids
                    print(
                        f"Node {self.node_id}: Updated file_replicas for {relative_file_path}: {self.file_replicas[relative_file_path]}")
                    print("Current file_replicas:", self.file_replicas)
                    self.broadcast_file_replicas_merge()  # Broadcast the update!
                else:
                    print(
                        f"Node {self.node_id}: No replicas successfully stored for {relative_file_path}, file_replicas not updated.")

            else:
                # If it's a forwarded request, just store locally if responsible
                if any(node[0] == self.host and int(node[1]) == CONTROL_PORT for node in responsible_nodes_info):
                    print(
                        f"Node {self.node_id}: Forwarded STOR, and this node is responsible, storing locally from temp.")
                    try:
                        final_file_path = file_path
                        shutil.copy2(temp_file_path, final_file_path)
                        print(
                            f"Node {self.node_id}: Successfully stored forwarded file locally: {file_path}")
                    except Exception as e:
                        print(
                            f"Node {self.node_id}: Error storing forwarded file locally: {e}")
                        # Consider better error reporting
                        client_socket.send(
                            b"550 Failed to store forwarded file locally.\r\n")

            # Inform client transfer complete after all distribution attempts
            client_socket.send(
                b"226 Transfer complete (file distributed to responsible nodes).\r\n")

        except Exception as e:
            print(f"Error in decentralized_handle_stor (main process): {e}")
            client_socket.send(b"550 Failed to store file.\r\n")
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

    def decentralized_handle_retr(self, client_socket, data_socket, filename, virtual_current_dir):
        """
        Handles the RETR command in a decentralized manner by downloading the file to a temp directory first
        if the current node is not responsible, and then sending it to the client.
        """
        print(
            f"Entering decentralized_handle_retr with filename: {filename}, virtual_current_dir: {virtual_current_dir}")
        if str(virtual_current_dir)[0] == "/":
            virtual_current_dir = str(virtual_current_dir)[1:]
            print(
                f"Removed leading slash from virtual_current_dir. Now: {virtual_current_dir}")
        if not data_socket:
            print("No data socket. Sending 425 error.")
            client_socket.send(b"425 Use PASV first.\r\n")
            return
        file_path = os.path.join(os.path.join(
            self.resources_dir, virtual_current_dir), filename)
        relative_file_path = os.path.join(virtual_current_dir, filename)
        print(f"file_path in RETR: {file_path}")
        print(f"relative_path in RETR: {relative_file_path}")

        print(f"self.file_replicas: {self.file_replicas}")
        key_to_lookup = str(relative_file_path)[1:] if str(
            relative_file_path)[0] == "/" else relative_file_path
        print(f"Looking up responsible nodes for key: {key_to_lookup}")
        responsible_node_ids = self.file_replicas.get(key_to_lookup)
        if responsible_node_ids is None:
            print(
                f"No responsible nodes found for key: {key_to_lookup}. Sending 550 File not found.")
            client_socket.send(b"550 File not found.\r\n")
            return

        print(
            f"Responsible node IDs for {relative_file_path}: {responsible_node_ids}")
        if self.node_id in responsible_node_ids:
            # Current node is responsible, retrieve locally
            print(
                f"Node {self.node_id} is responsible for key {relative_file_path}, retrieving locally.")
            try:
                client_socket.send(b"150 Opening data connection.\r\n")
                conn, addr = data_socket.accept()
                print(
                    f"Data connection accepted from {addr} for local retrieval.")
                if not os.path.exists(file_path):
                    print(
                        f"File not found locally at path: {file_path}. Sending 550 File not found.")
                    client_socket.send(b"550 File not found.\r\n")
                    return
                with open(file_path, 'rb') as file:
                    print(f"Opening file locally for reading: {file_path}")
                    while True:
                        data = file.read(BUFFER_SIZE)
                        if not data:
                            print("Finished reading file.")
                            break
                        print(f"Sending {len(data)} bytes to data connection.")
                        conn.send(data)
                    print("Data sending loop finished.")
                conn.close()
                print("Data connection closed after local retrieval.")
                client_socket.send(b"226 Transfer complete.\r\n")
                print("Sent 226 Transfer complete to client.")
            except Exception as e:
                print(f"Error in decentralized_handle_retr (local): {e}")
                client_socket.send(b"550 Failed to retrieve file.\r\n")
            finally:
                if data_socket:
                    data_socket.close()
                    print("Data socket closed in finally block (local retrieval).")
        else:
            # Another node is responsible, act as FTP client to retrieve and save to temp dir then send
            responsible_node_host = ""
            responsible_node_control_port = ""

            print("Current node is not responsible, forwarding request by downloading to temp.")
            for node_info in self.chord_nodes_config:
                if node_info[2] in responsible_node_ids and node_info[2] != self.node_id:
                    responsible_node_host = node_info[0]
                    responsible_node_control_port = node_info[1]
                    print(
                        f"Node at {responsible_node_host}:{responsible_node_control_port} is responsible for key {file_path}, forwarding RETR request via simplified FTP client and saving to temp.")
                    try:
                        client_socket.send(
                            b"150 Opening data connection from responsible node via simplified FTP.\r\n")

                        # Simplified FTP Client Logic (no USER/PASS)
                        ftp_client_socket = socket.socket(
                            socket.AF_INET, socket.SOCK_STREAM)
                        print(
                            f"Connecting to responsible node at {responsible_node_host}:{responsible_node_control_port}")
                        ftp_client_socket.connect(
                            (responsible_node_host, responsible_node_control_port))
                        print(f"Connected to responsible node.")
                        welcome_message = ftp_client_socket.recv(BUFFER_SIZE)
                        print(
                            f"Received welcome message from responsible node: {welcome_message}")
                        ftp_client_socket.send(b"PASV\r\n")
                        print("Sent PASV to responsible node.")
                        pasv_response = ftp_client_socket.recv(
                            BUFFER_SIZE).decode()
                        print(
                            f"Received PASV response from responsible node: {pasv_response}")
                        # Parse PASV response to get data port
                        ip_str = pasv_response.split('(')[1].split(')')[0]
                        ip_parts = ip_str.split(',')
                        data_server_ip = ".".join(ip_parts[:4])
                        data_server_port = (
                            int(ip_parts[4]) << 8) + int(ip_parts[5])
                        print(
                            f"Parsed data server IP: {data_server_ip}, Port: {data_server_port}")

                        ftp_data_socket_client = socket.socket(
                            socket.AF_INET, socket.SOCK_STREAM)
                        print(
                            f"Connecting data socket to {data_server_ip}:{data_server_port}")
                        ftp_data_socket_client.connect(
                            (data_server_ip, data_server_port))
                        print(f"Data socket connected to responsible node.")

                        ftp_client_socket.send(
                            f"FORWARD_RETR {relative_file_path}\r\n".encode())
                        print(
                            f"Sent FORWARD_RETR command: FORWARD_RETR {relative_file_path}")
                        retr_response = ftp_client_socket.recv(
                            BUFFER_SIZE).decode()  # 150 or error
                        print(
                            f"Received RETR response from responsible node: {retr_response}")

                        if retr_response.startswith("150"):
                            data_conn, addr = data_socket.accept()  # Accept client data connection
                            print(
                                f"Data connection accepted from client {addr} for forwarding from temp file.")

                            temp_file_path = ""
                            # Use tempfile.TemporaryDirectory to handle temp dir and cleanup
                            with tempfile.TemporaryDirectory() as temp_dir:
                                temp_file_path = os.path.join(
                                    temp_dir, "temp_downloaded_file")
                                print(f"Temporary file path: {temp_file_path}")
                                with open(temp_file_path, 'wb') as temp_file:
                                    print(
                                        "Receiving data from remote and writing to temp file...")
                                    while True:
                                        data_from_remote = ftp_data_socket_client.recv(
                                            BUFFER_SIZE)
                                        if not data_from_remote:
                                            print(
                                                "No more data from remote data socket.")
                                            break
                                        temp_file.write(data_from_remote)
                                        print(
                                            f"Received and wrote {len(data_from_remote)} bytes to temp file.")
                                    print("Finished writing to temp file.")

                                print("Opening temp file to send data to client...")
                                with open(temp_file_path, 'rb') as temp_file_read:
                                    print("Sending data from temp file to client...")
                                    while True:
                                        data_from_temp_file = temp_file_read.read(
                                            BUFFER_SIZE)
                                        if not data_from_temp_file:
                                            print("Finished reading from temp file.")
                                            break
                                        data_conn.sendall(data_from_temp_file)
                                        print(
                                            f"Sent {len(data_from_temp_file)} bytes to client from temp file.")
                                    print(
                                        "Finished sending data from temp file to client.")
                            # temp_dir is automatically deleted here

                            data_conn.close()  # Close client data connection
                            print(
                                "Client data connection closed after forwarding from temp file.")
                            ftp_data_socket_client.close()
                            print("Remote data socket client closed.")
                            response = ftp_client_socket.recv(
                                BUFFER_SIZE).decode()  # Get 226 or error
                            print(
                                f"Received final response from responsible node: {response}")
                            ftp_client_socket.close()
                            print("Control socket to responsible node closed.")

                            if response.startswith("226"):
                                client_socket.send(
                                    b"226 Transfer complete (forwarded via simplified FTP and temp file).\r\n")
                                print(
                                    "Sent 226 Transfer complete (forwarded via temp file) to client.")
                            else:
                                client_socket.send(
                                    b"550 Failed to forward file via simplified FTP (remote error after temp file).\r\n")
                                print(
                                    "Sent 550 Failed to forward (remote error after temp file) to client.")
                        else:
                            client_socket.send(
                                b"550 Failed to retrieve file via simplified FTP (remote RETR failed before temp file).\r\n")
                            print(
                                "Sent 550 Failed to retrieve (remote RETR failed before temp file) to client.")
                            ftp_client_socket.close()
                            ftp_data_socket_client.close()

                    except Exception as e:
                        print(
                            f"Error in decentralized_handle_retr (forwarding via simplified FTP with temp file): {e}")
                        client_socket.send(
                            b"550 Failed to retrieve file (forwarding error via simplified FTP with temp file).\r\n")
                        print(
                            "Sent 550 Failed to retrieve (forwarding error with temp file) to client.")
                    finally:
                        if data_socket:
                            data_socket.close()
                            print(
                                "Data socket closed in finally block (forwarding with temp file).")

    def decentralized_handle_dele(self, client_socket, filename):
        """
        Handles the FORWARD_DELE command to delete a file locally.
        """
        try:
            filepath = os.path.join(self.current_dir, filename)
            if os.path.exists(filepath):
                os.remove(filepath)
                client_socket.send(
                    b"250 File deleted successfully (forwarded).\r\n")
            else:
                client_socket.send(b"550 File not found (forwarded).\r\n")
        except Exception as e:
            print(f"Error in FORWARD_DELE: {e}")
            client_socket.send(b"550 Failed to delete file (forwarded).\r\n")

# FTP server methods (rest of FTP server methods remain mostly the same)
    # ... (handle_client, handle_user, handle_pwd, handle_cwd, handle_pasv, handle_list, handle_stor, handle_size, handle_mdtm, handle_mkd, handle_retr, handle_dele, handle_rmd, handle_rnfr, handle_rnto, start) ...

    def handle_client(self, client_socket):
        try:
            client_socket.send(b"220 Welcome to the FTP server.\r\n")
            data_socket = None
            virtual_current_dir = Path("/")
            while True:
                try:
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
                    elif cmd == "REPLICATE_DIR":
                        if self.create_folder(arg):
                            client_socket.send(
                                b"226 Replication of Folder Successfully.\r\n")
                    elif cmd =="COPY":
                        self.receive_file(client_socket, data_socket, arg)
                    elif cmd == "PASS":
                        client_socket.send(b"230 User logged in, proceed.\r\n")
                    elif cmd == "SYST":
                        client_socket.send(b"215 UNIX Type: L8\r\n")
                    elif cmd == "FEAT":
                        client_socket.send(
                            b"211-Features:\r\n UTF8\r\n211 End\r\n")
                    elif cmd == "PWD":
                        self.handle_pwd(client_socket, virtual_current_dir)
                    elif cmd == "CWD":
                        virtual_current_dir = self.handle_cwd(
                            client_socket, arg, virtual_current_dir)
                    elif cmd == "CDUP":
                        virtual_current_dir = self.handle_cwd(client_socket, Path(
                            self.current_dir).parent, virtual_current_dir)
                    elif cmd == "TYPE":
                        client_socket.send(b"200 Type set to I.\r\n")
                    elif cmd == "PASV":
                        data_socket = self.handle_pasv(client_socket)
                    elif cmd == "LIST":
                        self.handle_list(
                            client_socket, data_socket, virtual_current_dir)
                    # elif cmd == "FETCH_LISTING":
                    #     self.handle_fetch_listing(client_socket, data_socket,virtual_current_dir)
                    elif cmd == "STOR":
                        self.decentralized_handle_stor(
                            client_socket, data_socket, arg,virtual_current_dir)
                    elif cmd == "FORWARD_STOR":
                        # need to parse arg to get filename and virtual_current_dir example of arg: /games/Batman AK/batman.exe we have to get filename = batman.exe and virtual_current_dir = /games/Batman AK
                        filename,virtual_current_dir = self.parse_path_argument(arg)
                        
                        self.decentralized_handle_stor(
                            client_socket, data_socket, filename,virtual_current_dir, is_forwarded_request=True)
                    elif cmd == "FORWARD_DELE":
                        self.decentralized_handle_dele(client_socket, arg)
                    elif cmd == "SIZE":
                        self.handle_size(client_socket, arg)
                    elif cmd == "MDTM":
                        self.handle_mdtm(client_socket, arg)
                    elif cmd == "MKD":
                        self.handle_mkd(client_socket, arg,
                                        virtual_current_dir)
                    elif cmd == "RETR":
                        self.decentralized_handle_retr(
                            client_socket, data_socket, arg,virtual_current_dir)
                    elif cmd == "FORWARD_RETR":
                        # need to parse arg to get filename and virtual_current_dir example of arg: /games/Batman AK/batman.exe we have to get filename = batman.exe and virtual_current_dir = /games/Batman AK
                        filename, virtual_current_dir = self.parse_path_argument(
                            arg)

                        self.decentralized_handle_retr(
                            client_socket, data_socket, filename, virtual_current_dir)
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

    def parse_path_argument(self,arg):
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

    def create_folder(self, folder_path):
        # folder path variable is relative to self.resources_dir
        absolute_folder_path = os.path.join(self.resources_dir, folder_path)

        try:
            os.makedirs(absolute_folder_path, exist_ok=True)
            print(
                f"Folder '{folder_path}' created successfully at '{absolute_folder_path}'")
            return True
        except OSError as e:
            print(
                f"Error creating folder '{folder_path}' at '{absolute_folder_path}': {e}")
        return False

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

    def handle_pwd(self, client_socket, virtual_current_dir):
        response = f'257 "{virtual_current_dir}"\r\n'
        client_socket.send(response.encode())

    def handle_cwd(self, client_socket, path, virtual_current_dir):
        try:
            # check case path = ".." and avoir that virtual_current_dir superpases self.server_resource
            if path == "..":
                if str(virtual_current_dir) == "/":
                    client_socket.send(b"550 Directory not allowed.\r\n")
                    return virtual_current_dir
                else:
                    virtual_current_dir = Path(virtual_current_dir).parent
                    client_socket.send(
                        b"250 Directory successfully changed.\r\n")
                    return virtual_current_dir

            # Check if the path is absolute or relative
            if not os.path.isabs(path):
                path = os.path.join(virtual_current_dir, path)

            print(f"Changing directory to {path}")
            virtual_current_dir_path = Path(path).resolve()
            key_virtual_curr_dir = str(virtual_current_dir_path).removeprefix(
                "/")  # extract directory name

            if key_virtual_curr_dir not in self.folder_replicas:  # check if directory exists in folder_replicas
                # send error if not allowed
                client_socket.send(b"550 Directory not allowed.\r\n")
                return virtual_current_dir  # return old directory

            virtual_current_dir = virtual_current_dir_path  # only change if allowed
            client_socket.send(b"250 Directory successfully changed.\r\n")
            print(f"Current directory: {virtual_current_dir}")
            return virtual_current_dir
        except Exception as e:
            print(f"Error in CWD: {e}")
            client_socket.send(b"550 Failed to change directory.\r\n")
            return virtual_current_dir

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

    def handle_list(self, client_socket, data_socket, virtual_current_dir):
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return

        client_socket.send(b"150 Here comes the directory listing.\r\n")

        unique_listing_entries = set()  # Use a set to store unique entries

        for folder_path, replica_nodes in self.folder_replicas.items():
            if str(virtual_current_dir) == "/":
                # If virtual_current_dir is root, add all top-level files
                # Check if it's a top-level file (no slashes, or only one and at the start which is already handled as relative path)
                if "/" not in folder_path:
                    dirname = os.path.basename(folder_path)  # extract dirname
                    unique_listing_entries.add(dirname)  # add dirname
                continue

            print(f"Folder path: {folder_path}")
            print(f"Virtual current dir: {virtual_current_dir}")
            print(
                f"Checking if {folder_path} is a subfolder of {virtual_current_dir}")

            # Get virtual_current_dir as string without leading/trailing slashes
            v_current_dir_str = str(virtual_current_dir).strip("/")

            # Check if folder_path starts with virtual_current_dir (without leading slash) + "/"
            if str(folder_path).startswith(v_current_dir_str + "/"):

                # Convert to string for slicing and len
                relative_path = str(folder_path)[
                    len(v_current_dir_str) + 1:]
                if "/" not in relative_path:  # Check if there are no more "/" in the relative path, meaning it is a direct subfolder
                    # Add the folder if it is a direct subfolder
                    print(f"Adding folder: {folder_path}")
                    dirname = os.path.basename(folder_path)  # extract dirname
                    unique_listing_entries.add(dirname)  # add dirname

        for file_path, replica_nodes in self.file_replicas.items():
            if str(virtual_current_dir) == "/":
                # If virtual_current_dir is root, add all top-level files
                # Check if it's a top-level file (no slashes, or only one and at the start which is already handled as relative path)
                if "/" not in str(file_path):
                    unique_listing_entries.add(file_path)
                continue

            print(f"File path: {file_path}")
            print(f"Virtual current dir: {virtual_current_dir}")
            print(f"Checking if {file_path} is in {virtual_current_dir}")

            if str(file_path).startswith(str(virtual_current_dir) + "/"):
                relative_path = str(file_path)[len(
                    str(virtual_current_dir)) + 1:]
                if "/" not in relative_path:
                    filename = relative_path  # Filename is the relative path itself in this case
                    unique_listing_entries.add(filename)
            elif str(virtual_current_dir) != "/" and str(file_path).startswith(str(virtual_current_dir).removeprefix("/")):
                relative_path = str(file_path)[
                    len(str(virtual_current_dir))-1:]
                if relative_path.startswith("/"):
                    # remove the first "/" if it exists after removing virtual_current_dir
                    relative_path = relative_path[1:]
                if "/" not in relative_path:
                    filename = relative_path
                    unique_listing_entries.add(filename)

        try:
            conn, addr = data_socket.accept()
            # Iterate through the set of unique entries and send them
            for entry_info in unique_listing_entries:
                conn.send((entry_info + '\r\n').encode())
            conn.close()
            client_socket.send(b"226 Directory send OK.\r\n")

        except Exception as e:
            print(f"Error sending combined listing to client: {e}")
            client_socket.send(b"425 Can't open data connection.\r\n")
        finally:
            if data_socket:
                data_socket.close()

    def get_local_file_listing(self, virtual_current_dir):
        """
        Returns a list of filenames and foldernames in the current directory
        of this server.
        """
        entries_list = []
        # Ensure virtual_current_dir is relative to self.current_dir
        if os.path.isabs(virtual_current_dir):
            virtual_current_dir = os.path.relpath(virtual_current_dir, "/")
        full_path = Path(self.current_dir).joinpath(
            virtual_current_dir).resolve()
        print(f"Listing directory: {full_path}")
        if os.path.exists(full_path) and full_path.is_dir():
            entries = os.listdir(full_path)
            for entry in entries:
                if entry == "temp_downloads":
                    continue  # Skip temp_downloads directory
                entries_list.append(entry)
                print(f"Entry: {entry}")
        else:
            print(
                f"Directory does not exist or is not a directory: {full_path}")
        return entries_list

    def handle_size(self, client_socket, filename):
        try:
            size = os.path.getsize(os.path.join(
                self.current_dir, filename))
            client_socket.send(f"213 {size}\r\n".encode())
        except Exception as e:
            client_socket.send(b"550 Could not get file size.\r\n")

    def handle_mdtm(self, client_socket, filename):
        try:
            mtime = os.path.getmtime(os.path.join(
                self.current_dir, filename))
            mtimestr = time.strftime("%Y%m%d%H%M%S", time.gmtime(mtime))
            client_socket.send(f"213 {mtimestr}\r\n".encode())
        except Exception as e:
            client_socket.send(
                b"550 Could not get file modification time.\r\n")

    def handle_mkd(self, client_socket, dirname, virtual_current_dir):
        try:
            parent_v_dir_str = str(virtual_current_dir).lstrip(
                "/")  # remove initial / to match folder_replicas key
            responsible_nodes = self.folder_replicas.get(
                parent_v_dir_str, [])  # get nodes for parent dir

            created_locally = False
            remote_mkdir_success = False  # assume success until proven otherwise

            if not responsible_nodes:
                # in this case the client wants to create new directory in root directory which is valid
                new_directory_path = self.current_dir.joinpath(
                    virtual_current_dir.relative_to("/"), dirname)
                if new_directory_path.exists():
                    print("subrootfolder not registered in folder_replicas")
                    raise Exception
                # Create the directory locally
                try:
                    new_directory_path.mkdir(parents=True, exist_ok=False)
                    created_locally = True
                    # Update folder_replicas for the newly created directory
                    relative_new_dir_path = str(
                        virtual_current_dir.joinpath(dirname).relative_to("/"))
                    if relative_new_dir_path not in self.folder_replicas:
                        self.folder_replicas[relative_new_dir_path] = [
                            self.node_id]
                        self.broadcast_folder_replicas_merge()
                        print(
                            f"Registered new folder '{relative_new_dir_path}' in folder_replicas for node {self.node_id}")
                        created_locally = True
                    else:
                        print(
                            "Wtf, se creo una nueva carpeta yy no se guardo en folder_replicas")
                except OSError as e:
                    print(f"Error creating directory locally: {e}")
                    client_socket.send(
                        b"550 Failed to create directory locally.\r\n")
                    return  # exit if local creation fails

            for node_id in responsible_nodes:
                if node_id == self.node_id:
                    # Construct the full path for the new directory for local node
                    new_directory_path = self.current_dir.joinpath(
                        virtual_current_dir.relative_to("/"), dirname)

                    # Check if the directory already exists locally
                    if new_directory_path.exists():
                        client_socket.send(
                            b"550 Directory already exists.\r\n")
                        return  # Exit if directory exists locally

                    # Create the directory locally
                    try:
                        new_directory_path.mkdir(parents=True, exist_ok=False)
                        created_locally = True
                        # Update folder_replicas for the newly created directory
                        relative_new_dir_path = str(
                            virtual_current_dir.joinpath(dirname).relative_to("/"))
                        if relative_new_dir_path not in self.folder_replicas:
                            self.folder_replicas[relative_new_dir_path] = [
                                self.node_id]
                            self.broadcast_folder_replicas_merge()
                            print(
                                f"Registered new folder '{relative_new_dir_path}' in folder_replicas for node {self.node_id}")
                        else:
                            print(
                                "Wtf, se creo una nueva carpeta yy no se guardo en folder_replicas")
                    except OSError as e:
                        print(f"Error creating directory locally: {e}")
                        client_socket.send(
                            b"550 Failed to create directory locally.\r\n")
                        return  # exit if local creation fails

                else:
                    # Send remote MKD command to other responsible nodes
                    # pass virtual path for remote nodes
                    remote_v_dir = str(virtual_current_dir)
                    # Command for remote node
                    command = f"REMOTE_MKD {remote_v_dir} {dirname}"
                    # self.send_command_to_node(node_id, command)
                    # In real implementation, we should wait for response from remote nodes and handle failures.
                    # For now, assuming command is sent successfully.
                    # You might want to add error handling and response aggregation here in future.

            if created_locally or remote_mkdir_success:  # if created locally or assumed remote success
                client_socket.send(
                    f'257 "{dirname}" directory created\r\n'.encode())
            else:
                client_socket.send(
                    b"550 Failed to create directory on all nodes.\r\n")

        except FileExistsError:  # this should not be reached because of the exist check before mkdir
            client_socket.send(b"550 Directory already exists.\r\n")
        # general OS error not related to local mkdir, but to folder_replicas access etc.
        except OSError as e:
            print(f"Error processing MKD command: {e}")
            client_socket.send(b"550 Failed to create directory.\r\n")
        except Exception as e:
            print(f"Unexpected error in MKD: {e}")
            client_socket.send(b"550 Failed to create directory.\r\n")

    def handle_dele(self, client_socket, filename):
        """
        Handles the DELE command in a decentralized manner.
        Deletes the file from all servers that have a replica.
        """
        filepath = os.path.join(
            self.current_dir, filename)  # Construct full file path here
        relative_file_path = os.path.relpath(
            filepath, self.resources_dir)  # Get relative path

        if relative_file_path in self.file_replicas:  # Use relative_file_path as key
            # Use relative_file_path as key
            replica_nodes = self.file_replicas[relative_file_path]
            print(f"File {filename} replicas found at nodes: {replica_nodes}")
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
                            forward_socket.recv(BUFFER_SIZE)  # Welcome message
                            forward_socket.send(
                                f"FORWARD_DELE {filename}\r\n".encode())
                            response = forward_socket.recv(
                                BUFFER_SIZE).decode().strip()
                            print(
                                f"Response from node {node_id} for FORWARD_DELE: {response}")
                            if response.startswith("250"):
                                pass  # Successfully forwarded deletion
                            else:
                                print(
                                    f"FORWARD_DELE to node {node_id} failed.")
                        except Exception as e:
                            print(
                                f"Error forwarding DELE to node {node_id}: {e}")
                        finally:
                            forward_socket.close()

            # Delete the file from the current server as well
            try:

                if os.path.exists(filepath):
                    os.remove(filepath)
                    print(f"File {filename} deleted locally.")
                else:
                    print(f"File {filename} not found locally.")

                # Update file_replicas dictionary
                if relative_file_path in self.file_replicas:  # Use relative_file_path as key
                    del self.file_replicas[relative_file_path]
                    self.broadcast_file_replicas_delete(relative_file_path)

                client_socket.send(b"250 File deleted successfully.\r\n")

            except Exception as e:
                print(f"Error deleting file locally in DELE: {e}")
                client_socket.send(b"550 Failed to delete file.\r\n")

        else:  # File not tracked in replicas, attempt local delete anyway or send error. Let's attempt local delete for robustness.
            try:
                print(
                    "File not tracked in replicas, attempt local delete anyway or send error")
                if os.path.exists(filepath):
                    os.remove(filepath)
                    client_socket.send(
                        b"250 File deleted successfully (local only, not tracked for replication).\r\n")
                else:
                    client_socket.send(
                        b"550 File not found (not tracked for replication).\r\n")
            except Exception as e:
                print(
                    f"Error deleting file locally in DELE (not tracked): {e}")
                client_socket.send(
                    b"550 Failed to delete file (not tracked for replication).\r\n")

    def handle_rmd(self, client_socket, dirname):
        try:
            os.rmdir(os.path.join(self.current_dir, dirname))
            client_socket.send(b"250 Directory deleted successfully.\r\n")
        except Exception as e:
            print(f"Error in RMD: {e}")
            client_socket.send(b"550 Failed to delete directory.\r\n")

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

    def start(self):
        while True:
            client_sock, addr = self.server_socket.accept()
            print(f"Connection from {addr}")
            threading.Thread(target=self.handle_client,
                             args=(client_sock,)).start()

    def handle_fetch_listing(self, client_socket, data_socket):
        """
        Handles the FETCH_LISTING command. Sends the local file listing to the requesting server.
        """
        if not data_socket:
            # Should not happen in server-to-server comms, but for safety.
            client_socket.send(b"425 Use PASV first.\r\n")
            return

        client_socket.send(b"150 Here comes the directory listing.\r\n")

        try:
            conn, addr = data_socket.accept()
            entries_list = self.get_local_file_listing()
            for entry_info in entries_list:
                # Send each entry info to requesting server
                conn.send((entry_info + '\r\n').encode())
            conn.close()
            client_socket.send(b"226 Directory send OK.\r\n")
        except Exception as e:
            print(f"Error in handle_fetch_listing: {e}")
            client_socket.send(b"425 Can't open data connection.\r\n")
        finally:
            if data_socket:
                data_socket.close()


if __name__ == "__main__":
    if len(sys.argv) < 3:  # Expecting node_id, config_string, host_ip
        print("""Read txt""")
        sys.exit(1)

    node_id = int(sys.argv[1])
    host_ip = sys.argv[2]
    print(f"host_ip from command line: {host_ip}")

    ftp_server = FTPServer(host=host_ip, node_id=node_id)  # Pass string
    ftp_server.start()
