import os
import socket
import threading
import time
from pathlib import Path

CONTROL_PORT = 21
BUFFER_SIZE = 1024


class FTPServer:
    def __init__(self, host='127.0.0.1',dev=True):
        
        self.dev = True
        self.host = host
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((host, CONTROL_PORT))
        self.server_socket.listen(5)
        print(f"FTP Server listening on {host}:{CONTROL_PORT}")
        base_dir= Path(__file__).resolve().parent.parent
        self.resources_dir=base_dir.joinpath('./server_resources')
        self.current_dir = self.resources_dir
        print(self.current_dir)

    def handle_client(self, client_socket):
        client_socket.send(b"220 Welcome to the FTP server.\r\n")
        data_socket = None
        while True:
            command = client_socket.recv(BUFFER_SIZE).decode().strip()
            if not command:
                break
            if command == 'STOR' or command == 'CWD':
                print(f"Received command: {command}")
            cmd = command.split()[0].upper()
            arg = command[len(cmd):].strip()

            if cmd == "USER":
                client_socket.send(b"331 User name okay, need password.\r\n")
            elif cmd == "PASS":
                client_socket.send(b"230 User logged in, proceed.\r\n")
            elif cmd == "SYST":
                client_socket.send(b"215 UNIX Type: L8\r\n")
            elif cmd == "FEAT":
                client_socket.send(b"211-Features:\r\n UTF8\r\n211 End\r\n")
            elif cmd == "PWD":
                self.handle_pwd(client_socket)
            elif cmd == "CWD":
                self.handle_cwd(client_socket, arg)
            elif cmd == "TYPE":
                client_socket.send(b"200 Type set to I.\r\n")
            elif cmd == "PASV":
                data_socket = self.handle_pasv(client_socket)
            elif cmd == "LIST":
                self.handle_list(client_socket, data_socket)
            elif cmd == "STOR":
                self.handle_stor(client_socket, data_socket, arg)
            elif cmd == "SIZE":
                self.handle_size(client_socket, arg)
            elif cmd == "MDTM":
                self.handle_mdtm(client_socket, arg)
            elif cmd == "MKD":
                self.handle_mkd(client_socket, arg)
            elif cmd == "QUIT":
                client_socket.send(b"221 Goodbye.\r\n")
                break
            else:
                client_socket.send(b"502 Command not implemented.\r\n")
        client_socket.close()

    def handle_pwd(self, client_socket):
        response = f'257 "{self.current_dir}"\r\n'
        client_socket.send(response.encode())

    def handle_cwd(self, client_socket, path):
        print('CWD:')
        print(path)
        try:
            if(os.path.commonpath([path,self.resources_dir]) == self.resources_dir):
                os.chdir(path)
                self.current_dir = os.getcwd()
                client_socket.send(b"250 Directory successfully changed.\r\n")
                print("250 Directory successfully changed.")
        except Exception as e:
            print('550 Failed to change directory.')
            client_socket.send(b"550 Failed to change directory.\r\n")
        print('-------------------')

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

    def handle_stor(self, client_socket, data_socket, filepath):
        if not data_socket:
            client_socket.send(b"425 Use PASV first.\r\n")
            return
        if os.path.commonpath([filepath,self.resources_dir]) == self.resources_dir:
            try:
                client_socket.send(b"150 Ok to send data.\r\n")
                conn, addr = data_socket.accept()
                filename=filepath.split('\\')[-1]
                with open(filepath, 'wb') as file:

                    print('STOR Command Inspect:')
                    print(filepath)
                    print(self.current_dir)
                    print(filename)
                    print(os.path.join(self.current_dir, filename))
                    print('---------------')

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
                data_socket.close()
            finally:
                data_socket.close()
        else: 
            print(f"Error in STOR: Trying to store outside of designated server resources.")
            client_socket.send(b"550 Failed to store file.\r\n")
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

    def start(self):
        while True:
            client_sock, addr = self.server_socket.accept()
            print(f"Connection from {addr}")
            threading.Thread(target=self.handle_client,
                             args=(client_sock,)).start()


if __name__ == "__main__":
    ftp_server = FTPServer()
    ftp_server.start()

# Status:	Connecting to 127.0.0.1: 21...
# Status:	Connection established, waiting for welcome message...
# Response:	220 Welcome to the FTP server.
# Command:	AUTH TLS
# Response:	502 Command not implemented.
# Command:	AUTH SSL
# Response:	502 Command not implemented.
# Status:	Insecure server, it does not support FTP over TLS.
# Command:	USER anonymous
# Response:	331 User name okay, need password.
# Command:	PASS ** *******************
# Response:	230 User logged in , proceed.
# Command:	OPTS UTF8 ON
# Response:	502 Command not implemented.
# Status:	Logged in
# Status:	Retrieving directory listing...
# Status:	Directory listing of "C:\Franco\Proyects\JackRipper01\ftp-server\Decentralizated-FTP-Server\transfered_files\from_client" successful
