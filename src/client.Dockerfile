FROM ubuntu

WORKDIR /app

# Install FTP client
RUN apt-get update && apt-get install -y ftp && apt-get install -y iproute2

COPY test_using_docker_networking/routing_client.sh /app


RUN chmod +x /app/routing_client.sh

ENTRYPOINT [ "/app/routing_client.sh" ]