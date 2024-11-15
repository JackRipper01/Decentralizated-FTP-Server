import os
import socket
import threading
import time


def run_client():
    host = '127.0.0.1'  # The server's hostname or IP address
    port = 21          # The port used by the server

    # Create a socket object
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Connect to the server
    client_socket.connect((host, port))
    
    message = "Hello, Server!"  # Message to send to the server
    client_socket.sendall(message.encode())  # Send message to the server
    
    data = client_socket.recv(1024)  # Receive response from the server
    print(f"Received from server: {data.decode()}")  # Print the response

    client_socket.close()  # Close the connection

if __name__ == '__main__':
    run_client()

'''
TESTS:
copiar afuera de resources un archivo
copiar una carpeta(incluir todo el interior)
forming a folder tree inside of server_resources
'''    
# Tests:
