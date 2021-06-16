#!/bin/bash

if [ "$EUID" -ne 0 ]
    then echo "This should be ran with root privileges. Exiting..."
    exit
fi

echo_and_run() { echo "+ $*" ; "$@" ; }

SERVICE_FILES=$(find . -iname *.service)
for file in $SERVICE_FILES
do
    echo_and_run cp $file /lib/systemd/system/.
    echo_and_run systemctl daemon-reload
    echo_and_run systemctl enable $(basename $file)
done
