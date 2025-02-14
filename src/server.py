import os
import socket
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
GLOBAL_COMMS_PORT = 3000  # Port for UDP global server communication
NODE_DISCOVERY_INTERVAL = 2  # Interval in seconds for broadcasting hello messages
TEMP_DOWNLOAD_DIR_NAME = "temp_downloads"
# Define timeout, e.g., 3 times the broadcast interval
INACTIVE_NODE_TIMEOUT = NODE_DISCOVERY_INTERVAL * 3


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

        self.node_id = node_id  # Unique ID for this Chord node
        self.chord_nodes_config = []  # Initialize as empty list
        self_config = [self.host, CONTROL_PORT,
                       self.node_id, time.time()]  # timestamp
        self.chord_nodes_config.append(self_config)

        # Dictionary to track file replicas: {filename: [node_id1, node_id2, node_id3]}
        self.file_replicas = {}

        # Log it
        print(f"Node {self.node_id} initialized with self config: {self_config}")

        self.start_node_discovery_broadcast()
        self.start_listener()
        self.start_inactive_node_removal_thread()

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

    def start_listener(self):
        """Starts the listener thread."""
        thread = threading.Thread(
            target=self.listen_for_server_messages, daemon=True)
        thread.start()
        print(f"Node Discovery Listener thread started.")

    def start_inactive_node_removal_thread(self):
        """Starts the inactive node removal loop in a separate thread."""
        thread = threading.Thread(
            target=self.run_inactive_node_removal_loop, daemon=True)
        thread.start()
        print(
            f"Inactive Node Removal thread started. Checking every {INACTIVE_NODE_TIMEOUT} seconds.")

    def broadcast_hello_message(self):
        """Periodically broadcasts a UDP 'hello' message with node information."""
        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_BROADCAST, 1)  # Enable broadcasting
        # Using '<broadcast>'
        server_address = ('<broadcast>', GLOBAL_COMMS_PORT)

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
            time.sleep(NODE_DISCOVERY_INTERVAL)  # Broadcast every N seconds

    def broadcast_file_replicas_update(self):
        """Broadcasts a UDP message with the current file_replicas data."""
        broadcast_socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_socket.bind(('', 0))  # Bind to any available port

        message = self.serialize_file_replicas()  # Serialize file_replicas to string
        message = f"FILE_REPLICAS_UPDATE:{message}".encode(
            'utf-8')  # Add a prefix

        for _ in range(3):  # Send multiple times for reliability in UDP
            broadcast_socket.sendto(
                message, ('<broadcast>', GLOBAL_COMMS_PORT))
            time.sleep(0.1)  # small delay between broadcasts

        broadcast_socket.close()
        print(f"Node {self.node_id}: Broadcasted file_replicas update.")

    def listen_for_server_messages(self):
        """Listens for UDP 'hello' and 'file replicas update' messages from other nodes and updates node config."""
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_address = ("", GLOBAL_COMMS_PORT)  # Bind to all interfaces
        listen_socket.bind(listen_address)
        # print(
        #     f"Node {self.node_id} listening for hello messages on port {GLOBAL_COMMS_PORT}")

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
                                    # Defensive check: Ensure timestamp exists
                                    if len(existing_node) < 4:
                                        print(
                                            f"Warning: Node config for node {hello_node_id} is missing timestamp. Adding current timestamp.")
                                        # Add timestamp if missing
                                        existing_node.append(time.time())
                                    else:
                                        # Update timestamp
                                        existing_node[3] = time.time()
                                    break

                            if not node_exists and hello_node_id != self.node_id:  # Avoid adding self and duplicates
                                # Add timestamp for new node
                                new_node_info.append(time.time())
                                self.chord_nodes_config.append(new_node_info)
                                self.broadcast_file_replicas_update()
                                print(
                                    f"Node {self.node_id} discovered new node: {new_node_info}")
                                print(
                                    f"Current chord_nodes_config: {self.chord_nodes_config}")

                            else:
                                if hello_node_id != self.node_id:
                                    # print(
                                    #     f"Node {self.node_id} received hello from existing node: {new_node_info} (or self)")
                                    a = 3

                        except ValueError:
                            print(
                                f"Node {self.node_id} received invalid hello message - ValueError: '{message}' from {address}")
                    else:
                        print(
                            f"Node {self.node_id} received invalid hello message - format error: '{message}' from {address}")
                # Check for our prefix
                elif message.startswith("FILE_REPLICAS_UPDATE:"):
                    # Extract the file_replicas string
                    replicas_str = message[len("FILE_REPLICAS_UPDATE:"):]
                    updated_replicas = self.deserialize_file_replicas(
                        replicas_str)  # Deserialize it back to dict
                    if updated_replicas:
                        # Update file_replicas
                        self.file_replicas.update(updated_replicas)
                        print(
                            f"Node {self.node_id}: Received and merged file_replicas update from {address}.")
                        print("Current file_replicas:", self.file_replicas)
                else:
                    print(
                        f"Node {self.node_id} received non-hello message: '{message}' from {address}")

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

        for node_info in self.chord_nodes_config:
            node_id = node_info[2]
            last_seen_time = node_info[3]
            if node_id == self.node_id:  # Don't remove self
                continue

            if current_time - last_seen_time > INACTIVE_NODE_TIMEOUT:
                print(
                    f"Inactive node found: Node {node_info[2]} (last seen: {node_info[3]})")
                self.chord_nodes_config.remove(
                    node_info)  # Remove inactive nodes
                print(
                    f"Updated chord_nodes_config after remove inactive nodes: {self.chord_nodes_config}")

    def serialize_file_replicas(self):  # NEW
        """Serializes the file_replicas dictionary to a string format (filename:node_id1,node_id2,...;...)."""
        serialized_str = ""
        for filename, node_ids in self.file_replicas.items():
            serialized_str += f"{filename}:{','.join(map(str, node_ids))};"
        return serialized_str.rstrip(';')  # Remove trailing semicolon if any

    def deserialize_file_replicas(self, replicas_str):  # NEW
        """Deserializes the string representation back to a file_replicas dictionary."""
        replicas_dict = {}
        if not replicas_str:  # Handle empty string
            return replicas_dict

        for item in replicas_str.split(';'):
            parts = item.split(':', 1)  # Split only once at the first ':'
            if len(parts) == 2:  # Ensure it has both filename and node_ids
                filename = parts[0]
                node_ids_str = parts[1]
                node_ids = [int(node_id) for node_id in node_ids_str.split(
                    ',')] if node_ids_str else []  # Handle empty node_ids string
                replicas_dict[filename] = node_ids
        return replicas_dict


# Chord methods (No changes needed in Chord logic itself)


    def get_key(self, filename):
        """
        Generates a Chord key for a given filename using SHA1 hash.
        """
        return int(hashlib.sha1(filename.encode()).hexdigest(), 16) % 1024

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

# Decentralized FTP server methods

    def decentralized_handle_stor(self, client_socket, data_socket, filename, is_forwarded_request=False):
        """
        Handles the STOR command in a decentralized manner.
        Data is first stored in a temp file, then distributed to responsible nodes.
        """
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return

        key = self.get_key(filename)
        responsible_nodes_info = self.find_successors(
            key, self.chord_nodes_config, num_successors=3)

        # Ensure temp download directory exists
        temp_dir_path = self.resources_dir.joinpath(
            TEMP_DOWNLOAD_DIR_NAME)
        if not temp_dir_path.exists():
            temp_dir_path.mkdir(parents=True, exist_ok=True)

        # Unique temp filename could be improved
        temp_file_path = temp_dir_path.joinpath(f"temp_{filename}")
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
                            final_file_path = os.path.join(
                                self.current_dir, filename)
                            # Use shutil.copy2 for efficient local copy
                            shutil.copy2(temp_file_path, final_file_path)
                            print(
                                f"Node {self.node_id}: Successfully stored locally from temp file: {filename}")
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
                                f"FORWARD_STOR {filename}\r\n".encode())
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
                    self.file_replicas[filename] = responsible_node_ids
                    print(
                        f"Node {self.node_id}: Updated file_replicas for {filename}: {self.file_replicas[filename]}")
                    print("Current file_replicas:", self.file_replicas)
                    self.broadcast_file_replicas_update()  # Broadcast the update!
                else:
                    print(
                        f"Node {self.node_id}: No replicas successfully stored for {filename}, file_replicas not updated.")

            else:
                # If it's a forwarded request, just store locally if responsible
                if any(node[0] == self.host and int(node[1]) == CONTROL_PORT for node in responsible_nodes_info):
                    print(
                        f"Node {self.node_id}: Forwarded STOR, and this node is responsible, storing locally from temp.")
                    try:
                        final_file_path = os.path.join(
                            self.current_dir, filename)
                        shutil.copy2(temp_file_path, final_file_path)
                        print(
                            f"Node {self.node_id}: Successfully stored forwarded file locally: {filename}")
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

    def decentralized_handle_retr(self, client_socket, data_socket, filename):
        """
        Handles the RETR command in a decentralized manner using simplified FTP client for inter-node communication (no USER/PASS).
        """
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return

        key = self.get_key(filename)  # Calculate Chord key
        responsible_node_info = self.find_successor(
            key, self.chord_nodes_config)
        responsible_node_host, responsible_node_control_port, responsible_node_id, responsible_node_time = responsible_node_info

        if responsible_node_host == self.host and responsible_node_control_port == CONTROL_PORT:
            # Current node is responsible, retrieve locally
            print(
                f"Node {self.node_id} is responsible for key {key}, retrieving locally.")
            try:
                client_socket.send(b"150 Opening data connection.\r\n")
                conn, addr = data_socket.accept()
                filepath = os.path.join(self.current_dir, filename)
                if not os.path.exists(filepath):
                    client_socket.send(b"550 File not found.\r\n")
                    return
                with open(filepath, 'rb') as file:
                    while True:
                        data = file.read(BUFFER_SIZE)
                        if not data:
                            break
                        conn.send(data)
                conn.close()
                client_socket.send(b"226 Transfer complete.\r\n")
            except Exception as e:
                print(f"Error in decentralized_handle_retr (local): {e}")
                client_socket.send(b"550 Failed to retrieve file.\r\n")
            finally:
                if data_socket:
                    data_socket.close()
        else:
            # Another node is responsible, act as FTP client to retrieve (simplified - no USER/PASS)
            print(
                f"Node at {responsible_node_host}:{responsible_node_control_port} is responsible for key {key}, forwarding RETR request via simplified FTP client.")
            try:
                client_socket.send(
                    b"150 Opening data connection from responsible node via simplified FTP.\r\n")

                # Simplified FTP Client Logic (no USER/PASS)
                ftp_client_socket = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                ftp_client_socket.connect(
                    (responsible_node_host, responsible_node_control_port))
                ftp_client_socket.recv(BUFFER_SIZE)  # Welcome message
                # USER/PASS REMOVED
                ftp_client_socket.send(b"PASV\r\n")
                pasv_response = ftp_client_socket.recv(BUFFER_SIZE).decode()
                # Parse PASV response to get data port
                ip_str = pasv_response.split('(')[1].split(')')[0]
                ip_parts = ip_str.split(',')
                data_server_ip = ".".join(ip_parts[:4])
                data_server_port = (int(ip_parts[4]) << 8) + int(ip_parts[5])

                ftp_data_socket_client = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM)
                ftp_data_socket_client.connect(
                    (data_server_ip, data_server_port))

                ftp_client_socket.send(f"RETR {filename}\r\n".encode())
                retr_response = ftp_client_socket.recv(
                    BUFFER_SIZE).decode()  # 150 or error

                if retr_response.startswith("150"):
                    data_conn, addr = data_socket.accept()  # Accept client data connection

                    # Receive data from inter-node data connection and forward to client data connection
                    while True:
                        data_from_remote = ftp_data_socket_client.recv(
                            BUFFER_SIZE)
                        if not data_from_remote:
                            break
                        data_conn.sendall(data_from_remote)

                    data_conn.close()  # Close client data connection
                    ftp_data_socket_client.close()
                    response = ftp_client_socket.recv(
                        BUFFER_SIZE).decode()  # Get 226 or error
                    ftp_client_socket.close()

                    if response.startswith("226"):
                        client_socket.send(
                            b"226 Transfer complete (forwarded via simplified FTP).\r\n")
                    else:
                        client_socket.send(
                            b"550 Failed to forward file via simplified FTP (remote error).\r\n")  # Improve error reporting
                else:
                    client_socket.send(
                        b"550 Failed to retrieve file via simplified FTP (remote RETR failed).\r\n")  # RETR command itself failed
                    ftp_client_socket.close()
                    ftp_data_socket_client.close()

            except Exception as e:
                print(
                    f"Error in decentralized_handle_retr (forwarding via simplified FTP): {e}")
                client_socket.send(
                    b"550 Failed to retrieve file (forwarding error via simplified FTP).\r\n")
            finally:
                if data_socket:
                    data_socket.close()

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
            while True:
                try:
                    command = client_socket.recv(BUFFER_SIZE).decode().strip()
                    if not command:
                        break
                    print(f"Received command: {command}")
                    cmd = command.split()[0].upper()
                    arg = command[len(cmd):].strip()

                    if cmd == "USER":
                        res = self.handle_user(client_socket, arg)
                        client_socket.send(res)
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
                    elif cmd == "FETCH_LISTING":
                        self.handle_fetch_listing(client_socket, data_socket)
                    elif cmd == "STOR":
                        self.decentralized_handle_stor(
                            client_socket, data_socket, arg)
                    elif cmd == "FORWARD_STOR":
                        self.decentralized_handle_stor(
                            client_socket, data_socket, arg, is_forwarded_request=True)
                    elif cmd == "FORWARD_DELE":
                        self.decentralized_handle_dele(client_socket, arg)
                    elif cmd == "SIZE":
                        self.handle_size(client_socket, arg)
                    elif cmd == "MDTM":
                        self.handle_mdtm(client_socket, arg)
                    elif cmd == "MKD":
                        self.handle_mkd(client_socket, arg)
                    elif cmd == "RETR":
                        self.decentralized_handle_retr(
                            client_socket, data_socket, arg)
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

    def handle_list(self, client_socket, data_socket):
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return

        client_socket.send(b"150 Here comes the directory listing.\r\n")

        unique_listing_entries = set()  # Use a set to store unique entries

        # 1. Get local listing
        local_listing = self.get_local_file_listing()
        for entry in local_listing:
            unique_listing_entries.add(entry)  # Add local entries to the set

        # 2. Fetch listings from other servers
        for node_config in self.chord_nodes_config:
            node_host, node_control_port, node_id, node_time = node_config
            if node_id != self.node_id:  # Don't fetch from self again
                try:
                    # Establish control connection to the other server
                    remote_control_socket = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM)
                    remote_control_socket.connect(
                        (node_host, node_control_port))
                    remote_control_socket.recv(BUFFER_SIZE)

                    # Send PASV command to remote server
                    remote_control_socket.send(b"PASV\r\n")
                    pasv_response = remote_control_socket.recv(
                        BUFFER_SIZE).decode()
                    if not pasv_response.startswith("227"):
                        print(
                            f"PASV failed on remote server {node_id}: {pasv_response}")
                        continue

                    # Extract data port
                    ip_str, port_str = pasv_response.split('(')[1].split(')')[
                        0].split(',')[-2:]
                    remote_data_port = int(port_str) + int(ip_str) * 256
                    remote_data_host = node_host

                    # Connect data socket
                    remote_data_socket_client = socket.socket(
                        socket.AF_INET, socket.SOCK_STREAM)
                    remote_data_socket_client.connect(
                        (remote_data_host, remote_data_port))

                    # Send FETCH_LISTING command
                    remote_control_socket.send(b"FETCH_LISTING\r\n")
                    listing_response = remote_control_socket.recv(
                        BUFFER_SIZE).decode()
                    if not listing_response.startswith("150"):
                        print(
                            f"FETCH_LISTING init failed on remote server {node_id}: {listing_response}")
                        remote_control_socket.close()
                        remote_data_socket_client.close()
                        continue

                    # Receive directory listing
                    remote_listing = b""
                    while True:
                        chunk = remote_data_socket_client.recv(BUFFER_SIZE)
                        if not chunk:
                            break
                        remote_listing += chunk

                    remote_data_socket_client.close()
                    remote_control_socket.close()

                    # Decode and add remote listing to set
                    remote_listing_str = remote_listing.decode().strip()
                    remote_entries = remote_listing_str.split('\r\n')
                    for entry in remote_entries:  # Add remote entries to the set
                        if entry:  # Make sure not to add empty strings from split
                            unique_listing_entries.add(entry)

                    print(f"Fetched listing from node {node_id}")

                except Exception as e:
                    print(f"Error fetching listing from node {node_id}: {e}")
                    continue

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

    def get_local_file_listing(self):
        """
        Returns a list of filenames and foldernames in the current directory
        of this server.
        """
        entries_list = []
        entries = os.listdir(self.current_dir)
        for entry in entries:
            if entry == "temp_downloads":
                continue  # Skip temp_downloads directory
            entries_list.append(entry)  # Just append the name
        return entries_list

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
            if data_socket:
                data_socket.close()

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
            if data_socket:
                data_socket.close()

    def handle_dele(self, client_socket, filename):
        """
        Handles the DELE command in a decentralized manner.
        Deletes the file from all servers that have a replica.
        """
        if filename in self.file_replicas:
            replica_nodes = self.file_replicas[filename]
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
                filepath = os.path.join(self.current_dir, filename)
                if os.path.exists(filepath):
                    os.remove(filepath)
                    print(f"File {filename} deleted locally.")
                else:
                    print(f"File {filename} not found locally.")

                # Update file_replicas dictionary
                if filename in self.file_replicas:
                    if self.node_id in self.file_replicas[filename]:
                        self.file_replicas[filename].remove(self.node_id)
                        # If no replicas left
                        if not self.file_replicas[filename]:
                            del self.file_replicas[filename]
                        self.broadcast_file_replicas_update()  # Broadcast changes

                client_socket.send(b"250 File deleted successfully.\r\n")

            except Exception as e:
                print(f"Error deleting file locally in DELE: {e}")
                client_socket.send(b"550 Failed to delete file.\r\n")

        else:  # File not tracked in replicas, attempt local delete anyway or send error. Let's attempt local delete for robustness.
            try:
                print(
                    "File not tracked in replicas, attempt local delete anyway or send error")
                filepath = os.path.join(self.current_dir, filename)
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


if __name__ == "__main__":
    if len(sys.argv) < 3:  # Expecting node_id, config_string, host_ip
        print("""Read txt""")
        sys.exit(1)

    node_id = int(sys.argv[1])
    host_ip = sys.argv[2]
    print(f"host_ip from command line: {host_ip}")

    ftp_server = FTPServer(host=host_ip, node_id=node_id)  # Pass string
    ftp_server.start()
