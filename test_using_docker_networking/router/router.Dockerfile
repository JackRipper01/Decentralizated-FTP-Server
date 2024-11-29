FROM alpine

RUN echo "net.ipv4.ip_forward=1" | tee -a /etc/sysctl.conf

CMD while true; do sleep 1; done