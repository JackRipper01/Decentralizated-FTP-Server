import os
import socket
import threading
import time
from pathlib import Path
import threading

CONTROL_PORT = 21
BUFFER_SIZE = 1024


class FTPServer:
    def __init__(self, host='10.0.10.3', dev=True):

        self.dev = True
        self.host = host
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(('0.0.0.0', CONTROL_PORT))
        self.server_socket.listen(5)
        print(f"FTP Server listening on {host}:{CONTROL_PORT}")
        base_dir = Path(__file__).resolve().parent.parent
        self.resources_dir = base_dir.joinpath('./server_resources')
        if not self.resources_dir.exists():
            self.resources_dir.mkdir()
            print(f"Created directory: {self.resources_dir}")
        else:
            print(f"Directory already exists: {self.resources_dir}")
        self.current_dir = self.resources_dir
        print(self.current_dir)

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
                    elif cmd == "STOR":
                        self.handle_stor(client_socket, data_socket, arg)
                    elif cmd == "SIZE":
                        self.handle_size(client_socket, arg)
                    elif cmd == "MDTM":
                        self.handle_mdtm(client_socket, arg)
                    elif cmd == "MKD":
                        self.handle_mkd(client_socket, arg)
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
                        break
                    else:
                        client_socket.send(
                            b"502 Command: %s not implemented.\r\n" % cmd.encode())
                except ConnectionResetError:
                    print(
                        "An existing connection was forcibly closed by one remote host .")
                    break
        finally:
            client_socket.close()

    def handle_user(self, client_socket, username):
        return b"331 User name okay, need password.\r\n"

    def handle_pwd(self, client_socket):
        response = f'257 "{self.current_dir}"\r\n'
        client_socket.send(response.encode())

    def handle_cwd(self, client_socket, path):
        try:
            os.chdir(path)
            self.current_dir = os.getcwd()
            client_socket.send(b"250 Directory successfully changed.\r\n")
        except Exception as e:
            client_socket.send(b"550 Failed to change directory.\r\n")

    def handle_pasv(self, client_socket):
        data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_socket.bind(('0.0.0.0', 0))
        data_socket.listen(1)
        ip, port = data_socket.getsockname()
        container_ip = socket.gethostbyname(socket.gethostname())
        ip_parts = container_ip.split('.')

        # ip_parts = self.host.split('.') with this change, the passive mode seems to work but ftp> ls returns 421 Service not available, remote server has closed connection.
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
                    file_info = f'{os.stat(full_path).st_size} {time.strftime("%b %d %H:%M", time.gmtime(os.stat(full_path).st_mtime))} {entry}\r\n'
                else:
                    # Format for files
                    file_info = f'{os.stat(full_path).st_size} {time.strftime("%b %d %H:%M", time.gmtime(os.stat(full_path).st_mtime))} {entry}\r\n'

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
            data_socket.close()

    def handle_dele(self, client_socket, filename):
        try:
            os.remove(os.path.join(self.current_dir, filename))
            client_socket.send(b"250 File deleted successfully.\r\n")
        except Exception as e:
            print(f"Error in DELE: {e}")
            client_socket.send(b"550 Failed to delete file.\r\n")

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
    import sys
    host = sys.argv[1] if len(sys.argv) > 1 else '10.0.10.3'
    ftp_server = FTPServer(host=host)
    ftp_server.start()
