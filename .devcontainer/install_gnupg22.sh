#!/bin/bash

yum-builddep -y gnupg2 
mkdir -p /tmp/gnupg22 && cd /tmp/gnupg22
gpg --list-keys
gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 249B39D24F25E3B6 04376F3EE0856959 2071B08A33BD3F06 8A861B1C7EFD60D9

wget -c https://www.gnupg.org/ftp/gcrypt/libgpg-error/libgpg-error-1.31.tar.gz.sig && \
wget -c https://www.gnupg.org/ftp/gcrypt/libgpg-error/libgpg-error-1.31.tar.gz && \
wget -c https://www.gnupg.org/ftp/gcrypt/libgcrypt/libgcrypt-1.8.3.tar.gz && \
wget -c https://www.gnupg.org/ftp/gcrypt/libgcrypt/libgcrypt-1.8.3.tar.gz.sig && \
wget -c https://www.gnupg.org/ftp/gcrypt/libassuan/libassuan-2.5.1.tar.bz2 && \
wget -c https://www.gnupg.org/ftp/gcrypt/libassuan/libassuan-2.5.1.tar.bz2.sig && \
wget -c https://www.gnupg.org/ftp/gcrypt/libksba/libksba-1.3.5.tar.bz2 && \
wget -c https://www.gnupg.org/ftp/gcrypt/libksba/libksba-1.3.5.tar.bz2.sig && \
wget -c https://www.gnupg.org/ftp/gcrypt/npth/npth-1.5.tar.bz2 && \
wget -c https://www.gnupg.org/ftp/gcrypt/npth/npth-1.5.tar.bz2.sig && \
wget -c https://www.gnupg.org/ftp/gcrypt/gnupg/gnupg-2.2.9.tar.bz2 && \
wget -c https://www.gnupg.org/ftp/gcrypt/gnupg/gnupg-2.2.9.tar.bz2.sig && \
gpg --verify libgpg-error-1.31.tar.gz.sig && tar -xzf libgpg-error-1.31.tar.gz && \
gpg --verify libgcrypt-1.8.3.tar.gz.sig && tar -xzf libgcrypt-1.8.3.tar.gz && \
gpg --verify libassuan-2.5.1.tar.bz2.sig && tar -xjf libassuan-2.5.1.tar.bz2 && \
gpg --verify libksba-1.3.5.tar.bz2.sig && tar -xjf libksba-1.3.5.tar.bz2 && \
gpg --verify npth-1.5.tar.bz2.sig && tar -xjf npth-1.5.tar.bz2 && \
gpg --verify gnupg-2.2.9.tar.bz2.sig && tar -xjf gnupg-2.2.9.tar.bz2 && \
cd libgpg-error-1.31/ && ./configure && make && make install && cd ../ && \
cd libgcrypt-1.8.3 && ./configure && make && make install && cd ../ && \
cd libassuan-2.5.1 && ./configure && make && make install && cd ../ && \
cd libksba-1.3.5 && ./configure && make && make install && cd ../ && \
cd npth-1.5 && ./configure && make && make install && cd ../ && \

cd gnupg-2.2.9 && ./configure && make && make install && \
echo "/usr/local/lib" > /etc/ld.so.conf.d/gpg2.conf && ldconfig -v && \

if [ -d ~/.gnugp ]; then rm -ri ~/.gnugp; fi
gpgconf --kill gpg-agent

rm -rf cd /tmp/gnupg22