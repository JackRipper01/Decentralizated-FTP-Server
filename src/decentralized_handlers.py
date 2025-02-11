import os
import socket
import threading
import time
import sys
from pathlib import Path
import hashlib
from static_config import *

# Decentralized FTP server methods
class handler:
    def decentralized_handle_stor(self, client_socket, data_socket, filename):
        """
        Handles the STOR command in a decentralized manner using simplified FTP client for inter-node communication (no USER/PASS).
        """
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return

        key = self.get_key(filename)  # Calculate Chord key
        responsible_node_info = self.find_successor(
            key, self.chord_nodes_config)
        responsible_node_host, responsible_node_control_port, responsible_node_id = responsible_node_info

        if responsible_node_host == self.host and responsible_node_control_port == CONTROL_PORT:
            # Current node is responsible, store locally
            print(
                f"Node {self.node_id} is responsible for key {key}, storing locally.")
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
                print(f"Error in decentralized_handle_stor (local): {e}")
                client_socket.send(b"550 Failed to store file.\r\n")
            finally:
                if data_socket:
                    data_socket.close()

        else:
            # Another node is responsible, act as FTP client to forward (simplified - no USER/PASS)
            print(
                f"Node at {responsible_node_host}:{responsible_node_control_port} is responsible for key {key}, forwarding STOR request via simplified FTP client.")
            try:
                client_socket.send(
                    b"150 Forwarding data to responsible node via simplified FTP.\r\n")
                conn, addr = data_socket.accept()

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

                ftp_client_socket.send(f"STOR {filename}\r\n".encode())
                ftp_client_socket.recv(BUFFER_SIZE)  # 150 response

                # Forward data from client data connection to inter-node data connection
                while True:
                    data_from_client = conn.recv(BUFFER_SIZE)
                    if not data_from_client:
                        break
                    ftp_data_socket_client.sendall(data_from_client)

                ftp_data_socket_client.close()
                conn.close()  # Close client data connection
                response = ftp_client_socket.recv(
                    BUFFER_SIZE).decode()  # Get 226 or error
                ftp_client_socket.close()

                if response.startswith("226"):
                    client_socket.send(
                        b"226 Transfer complete (forwarded via simplified FTP).\r\n")
                else:
                    client_socket.send(
                        b"550 Failed to forward file via simplified FTP.\r\n")  # Improve error reporting

            except Exception as e:
                print(
                    f"Error in decentralized_handle_stor (forwarding via simplified FTP): {e}")
                client_socket.send(
                    b"550 Failed to store file (forwarding error via simplified FTP).\r\n")
            finally:
                if data_socket:
                    data_socket.close()

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
        responsible_node_host, responsible_node_control_port, responsible_node_id = responsible_node_info

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
