ARG GLUE_IMAGE_VERSION=3.0.0_image_01

FROM amazon/aws-glue-libs:glue_libs_${GLUE_IMAGE_VERSION}

ARG USERNAME=glue_user
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG LOCAL_DEV=true

ENV AWS_REGION=eu-central-1

ENV SCALA_VERSION=2.12.17
ENV SCALA_BINARY_VERSION=2.12
ENV SBT_VERSION=1.2.8

USER root

RUN yum install -y \
    curl \
    git \
    sudo \
    telnet \
    unzip \
    vim \
    zip

RUN usermod -aG wheel $USERNAME && \
    echo "%wheel ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers

ADD .devcontainer/install_gnupg22.sh /usr/local/bin/
RUN test "$LOCAL_DEV" == "TRUE" && \
    yum install -y \
    gnupg2 && \
    bash /usr/local/bin/install_gnupg22.sh || bash -c true

RUN test "$LOCAL_DEV" != "TRUE" && \
    mkdir -p /data /data/target /data/project/target && chown -R $USERNAME:$USER_GID /data || bash -c true

USER $USERNAME
WORKDIR /workspace

# Fix MongoDB bug
RUN mv /home/glue_user/aws-glue-libs/jars/mongo-java-driver-3.10.1.jar /home/glue_user/aws-glue-libs/jars/mongo-java-driver-3.10.1.jar.old

# Other stuff
ADD .devcontainer/init_container.sh /usr/local/bin/

ADD .devcontainer/hive-site.xml /home/glue_user/spark/conf/hive-site.xml.local

USER root

# Install JDK, Scala and SBT
RUN yum install -y java-1.8.0-amazon-corretto-devel

RUN curl -fsL "https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz" | tar xfz - -C /usr/share && \
    chown -R root:root /usr/share/sbt && \
    chmod -R 755 /usr/share/sbt && \
    ln -s /usr/share/sbt/bin/sbt /usr/local/bin/sbt

RUN curl -fsL https://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /usr/share && \
    mv /usr/share/scala-$SCALA_VERSION /usr/share/scala && \
    chown -R root:root /usr/share/scala && \
    chmod -R 755 /usr/share/scala && \
    ln -s /usr/share/scala/bin/* /usr/local/bin

ENV SBT_OPTS="-Xms512M -Xmx2G -Xss2M -XX:MaxMetaspaceSize=1G"



# Clean up
USER $USERNAME