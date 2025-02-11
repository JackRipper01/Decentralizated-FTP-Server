FROM alpine

# Copy the server_resources folder into the container
COPY server_resources /server_resources
WORKDIR /app

COPY test_using_docker_networking/routing.sh /app
COPY src/server.py /app


RUN chmod +x /app/routing.sh



ENTRYPOINT [ "/app/routing.sh" ]