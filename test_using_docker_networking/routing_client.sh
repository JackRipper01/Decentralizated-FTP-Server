#!/bin/sh

First=$(hostname -i | cut -d . -f1)
Second=$(hostname -i | cut -d . -f2)
Third=$(hostname -i | cut -d . -f3)
Router=$First"."$Second"."$Third".254"

ip route del default
ip route add default via $Router

# Keep the container running
tail -f /dev/null

