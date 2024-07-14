#!/bin/sh
#
# DON'T EDIT THIS!
#
# CodeCrafters uses this file to test your code. Don't make any changes here!
#
# DON'T EDIT THIS!
set -e
echo "Building server..."
cd ~/dev/diy-redis
cmake . #>/dev/null
echo "Running Make..."
make #>/dev/null
echo "Running..."
exec ./server "$@"
