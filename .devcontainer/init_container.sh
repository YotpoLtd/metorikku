#!/bin/bash

INIT_FILE="$HOME/.devcontainer_initiated"

if [[ ! -f "$INIT_FILE" ]]; then    
    rm -Rf $HOME/.ssh && mkdir $HOME/.ssh && cp -Rf /tmp/.ssh/* $HOME/.ssh && chmod 400 $HOME/.ssh/*    

    stty -echo
    gpg --list-keys    
    gpg --import "/tmp/.gnupg/public.key"
    gpg --import "/tmp/.gnupg/private.key"
    stty echo
    
    sed -i '/SetEnv/d' "$HOME/.ssh/config"
    
    touch "$INIT_FILE"
fi
