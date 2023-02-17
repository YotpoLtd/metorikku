#!/bin/bash

INIT_FILE="$HOME/.devcontainer_initiated"

update_cas(){
    sudo bash -c "
openssl s_client -showcerts -verify 5 -connect github.com:443 < /dev/null |
 awk '/BEGIN CERTIFICATE/,/END CERTIFICATE/{ if(/BEGIN CERTIFICATE/){a++}; out=\"/usr/local/share/ca-certificates/cert\"a\".crt\"; print >out}' 

update-ca-certificates
"
}

if [[ ! -f "$INIT_FILE" ]]; then    
    rm -Rf $HOME/.ssh && mkdir $HOME/.ssh && cp -Rf /tmp/.ssh/* $HOME/.ssh && chmod 400 $HOME/.ssh/*    

    stty -echo
    gpg --list-keys    
    gpg --import "/tmp/.gnupg/public.key"
    gpg --import "/tmp/.gnupg/private.key"
    stty echo
    
    sed -i '/SetEnv/d' "$HOME/.ssh/config"

    update_cas
    
    touch "$INIT_FILE"
fi
