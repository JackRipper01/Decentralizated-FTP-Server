docker run -d --rm --name client1 -e PYTHONUNBUFFERED=1 --cap-add NET_ADMIN --network clients ftp-client
docker run -d --rm --name client2 -e PYTHONUNBUFFERED=1 --cap-add NET_ADMIN --network clients ftp-client "10.0.10.2"
docker run -d --rm --name server -e PYTHONUNBUFFERED=1 --cap-add NET_ADMIN --network servers ftp-server "10.0.10.3"
