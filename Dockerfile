FROM python:alpine
COPY . /ftp-server
WORKDIR /ftp-server
EXPOSE 1456
CMD ["python", "server.py"]