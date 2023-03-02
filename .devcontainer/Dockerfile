ARG GLUE_IMAGE_VERSION=4.0.0_image_01

FROM amazon/aws-glue-libs:glue_libs_${GLUE_IMAGE_VERSION}

ARG USERNAME=glue_user
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG LOCAL_DEV=TRUE

SHELL ["/bin/bash", "-c"]

USER root

RUN yum install -y curl \
    git \
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

ADD .devcontainer/install_gnupg22.sh /usr/local/bin/
RUN test "$LOCAL_DEV" == "TRUE" && \
    yum install -y \
        gnupg2 && \
    bash /usr/local/bin/install_gnupg22.sh || bash -c true

USER $USERNAME
WORKDIR "/home/${USERNAME}"
ENV HOME="/home/${USERNAME}"

# Fix MongoDB bug
RUN rm -f /home/${USERNAME}/spark/jars/*mongo* /home/${USERNAME}/spark/jars/*bson* \
          /home/${USERNAME}/aws-glue-libs/jars/*mongo* /home/${USERNAME}/aws-glue-libs/jars/*bson*

# Install Java & related
RUN sudo yum install -y java-1.8.0-amazon-corretto-devel

RUN curl -s "https://get.sdkman.io?rcupdate=false" | bash

ARG SCALA_VERSION="2.12.17"
ENV SCALA_BINARY_VERSION="2.12"
ARG SBT_VERSION="1.8.0"

RUN source "${HOME}/.sdkman/bin/sdkman-init.sh" \
    && sdk install scala "$SCALA_VERSION" \
    && sdk install sbt "$SBT_VERSION"

# Other stuff
ADD .devcontainer/init_container.sh /usr/local/bin/

ADD .devcontainer/hive-site.xml ${HOME}/spark/conf/hive-site.xml

# Configure profile
RUN echo '' >> "${HOME}/.bashrc"
RUN echo 'export PATH="${PATH}":${HOME}/.local/bin' >> "${HOME}/.bashrc"
RUN echo 'export AWS_REGION=eu-central-1' >> "${HOME}/.bashrc"
RUN echo '[[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && source "$HOME/.sdkman/bin/sdkman-init.sh"' >> "${HOME}/.bashrc"
RUN echo 'export SBT_OPTS="-Xms512M -Xmx2G -Xss2M -XX:MaxMetaspaceSize=1G"' >> "${HOME}/.bashrc"