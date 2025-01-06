#!/bin/bash

# TODO: Delete this script once the initContainers have been removed from
#       all repo instances.
python3 /usr/bin/migrate.py /data/repo/osg/23-* /data/repo/osg/24-*
exit 0
