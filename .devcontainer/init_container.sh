#!/bin/bash

INIT_FILE="$HOME/.devcontainer_initiated"

if [[ ! -f "$INIT_FILE" ]]; then    
    rm -Rf $HOME/.ssh && mkdir $HOME/.ssh && cp -Rf /tmp/.ssh/* $HOME/.ssh && chmod 400 $HOME/.ssh/*    

    stty -echo
    gpg --list-keys    
    gpg --import "/tmp/.gnupg/public.key"
    gpg --import "/tmp/.gnupg/private.key"
    stty echo

    ln -s /home/${USER}/.cache/pypoetry/virtualenvs/* /home/${USER}/.cache/pypoetry/virtualenvs/venv

    mv /home/glue_user/spark/conf/hive-site.xml /home/glue_user/spark/conf/hive-site.xml.glue
    mv /home/glue_user/spark/conf/hive-site.xml.local /home/glue_user/spark/conf/hive-site.xml
    
    touch "$INIT_FILE"
fi
