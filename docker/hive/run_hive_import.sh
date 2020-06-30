#!/usr/bin/expect -f

set timeout -1

spawn /init-hive.sh
spawn /opt/atlas/hook-bin/import_hive.sh

expect "Enter username for atlas :- "

send -- "$env(ADMIN_USERNAME)\n"

expect "Enter password for atlas :- "

send -- "$env(ADMIN_PASSWORD)\n"

expect eof
