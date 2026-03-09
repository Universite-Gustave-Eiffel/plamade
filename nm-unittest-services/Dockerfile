FROM nathanhess/slurm:base

USER root

# 1. Install Java 21, SSH, and Sudo
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    openssh-server \
    unzip \
    rsync \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# 2. Basic SSH setup
RUN mkdir -p /var/run/sshd && \
    sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config

# 3. Create the initialization script
RUN echo '#!/bin/bash\n\
USERNAME=${USER_NAME:-testuser}\n\
\n\
# 1. Create User if it doesn't exist\n\
if ! id -u "$USERNAME" >/dev/null 2>&1; then\n\
    useradd -m -s /bin/bash "$USERNAME"\n\
    echo "User $USERNAME created."\n\
fi\n\
\n\
# 2. Handle Sudo Access\n\
if [ "$SUDO_ACCESS" = "true" ]; then\n\
    echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/$USERNAME\n\
fi\n\
\n\
# 3. Handle SSH Key\n\
if [ -n "$PUBLIC_KEY" ]; then\n\
    USER_HOME=$(eval echo ~$USERNAME)\n\
    mkdir -p "$USER_HOME/.ssh"\n\
    echo "$PUBLIC_KEY" > "$USER_HOME/.ssh/authorized_keys"\n\
    chown -R "$USERNAME:$USERNAME" "$USER_HOME/.ssh"\n\
    chmod 700 "$USER_HOME/.ssh"\n\
    chmod 600 "$USER_HOME/.ssh/authorized_keys"\n\
    echo "SSH Public Key injected for $USERNAME."\n\
fi\n\
\n\
# 4. Handle Password Access\n\
if [ "$PASSWORD_ACCESS" = "false" ]; then\n\
    sed -i "s/^#\?PasswordAuthentication.*/PasswordAuthentication no/" /etc/ssh/sshd_config\n\
else\n\
    sed -i "s/^#\?PasswordAuthentication.*/PasswordAuthentication yes/" /etc/ssh/sshd_config\n\
fi\n\
\n\
# 5. Start SSH service\n\
service ssh start\n\
\n\
# 6. Execute the Slurm startup (not using exec so we can keep the script running)\n\
/etc/startup.sh "$@"\n\
\n\
echo "All services started. Keeping container alive..."\n\
# 7. Keep the container running forever\n\
tail -f /dev/null' > /usr/local/bin/init-ssh-slurm.sh

RUN chmod +x /usr/local/bin/init-ssh-slurm.sh

USER root
ENTRYPOINT ["/usr/local/bin/init-ssh-slurm.sh"]
