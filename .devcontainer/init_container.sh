#!/bin/bash

INIT_FILE="$HOME/.devcontainer_initiated"

update_cas(){
    mkdir -p /tmp/certs

    openssl s_client -showcerts -verify 5 -connect github.com:443 < /dev/null | \
        awk '/BEGIN CERTIFICATE/,/END CERTIFICATE/{ if(/BEGIN CERTIFICATE/){a++}; out="/tmp/certs/cert"a".crt"; print >out}' 

    sudo cp /tmp/certs/*.crt /etc/pki/ca-trust/source/anchors/
    sudo update-ca-trust
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
