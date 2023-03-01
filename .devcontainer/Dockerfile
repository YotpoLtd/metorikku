ARG GLUE_IMAGE_VERSION=4.0.0_image_01

FROM amazon/aws-glue-libs:glue_libs_${GLUE_IMAGE_VERSION}

ARG USERNAME=glue_user
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG LOCAL_DEV=true

SHELL ["/bin/bash", "-c"]

USER root

RUN yum install -y curl \
    git \
    gnupg2 \
    jq \
    software-properties-common \
    telnet \
    unzip \
    vim \
    wget \
    zip

# Install sudo
RUN yum install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

RUN test "$LOCAL_DEV" != "TRUE" && \
    mkdir -p /data && chown -R $USERNAME:$USER_GID /data || bash -c true

USER $USERNAME
WORKDIR "/home/${USERNAME}"
ENV HOME="/home/${USERNAME}"

# Fix MongoDB bug
RUN mv ${HOME}/aws-glue-libs/jars/mongo-java-driver-3.10.1.jar ${HOME}/aws-glue-libs/jars/mongo-java-driver-3.10.1.jar.old

# Install Java & related
RUN curl -s "https://get.sdkman.io?rcupdate=false" | bash

ARG SCALA_VERSION="2.12.17"
ENV SCALA_BINARY_VERSION="2.12"
ARG SBT_VERSION="1.8.0"

RUN source "${HOME}/.sdkman/bin/sdkman-init.sh" \
    && sdk install scala "$SCALA_VERSION" \
    && sdk install sbt "$SBT_VERSION"

# Other stuff
ADD .devcontainer/init_container.sh /usr/local/bin/

# Configure profile
RUN echo '' >> "${HOME}/.bashrc"
RUN echo 'export PATH="${PATH}":${HOME}/.local/bin' >> "${HOME}/.bashrc"
RUN echo 'export AWS_REGION=eu-central-1' >> "${HOME}/.bashrc"
RUN echo '[[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && source "$HOME/.sdkman/bin/sdkman-init.sh"' >> "${HOME}/.bashrc"
RUN echo 'export SBT_OPTS="-Xms512M -Xmx2G -Xss2M -XX:MaxMetaspaceSize=1G"' >> "${HOME}/.bashrc"