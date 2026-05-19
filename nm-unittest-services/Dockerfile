FROM nathanhess/slurm:base

USER root

# Install Java 21, SSH, apptainer, and Sudo
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    openssh-server \
    unzip \
    rsync \
    sudo \
    software-properties-common \
    curl \
    && add-apt-repository -y ppa:apptainer/ppa \
    && apt-get update && apt-get install -y apptainer-suid \
    && rm -rf /var/lib/apt/lists/*

# (Optional) Verify installation
RUN apptainer --version

# Basic SSH setup
RUN mkdir -p /var/run/sshd && \
    sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config

# Create the global scratch directory
RUN mkdir -p /scratch && chmod 777 /scratch

# Add the Prolog script to the container
COPY prolog.sh /etc/slurm/prolog.sh
RUN chmod +x /etc/slurm/prolog.sh

# Update slurm.conf to point to the Prolog script
# We append the configuration to the end of the existing slurm.conf
RUN echo "Prolog=/etc/slurm/prolog.sh" >> /etc/slurm/slurm.conf

# Add an Epilog script to clean up after the job
RUN echo '#!/bin/bash\nrm -rf /scratch/job.${SLURM_JOB_ID}' > /etc/slurm/epilog.sh && \
    chmod +x /etc/slurm/epilog.sh && \
    echo "Epilog=/etc/slurm/epilog.sh" >> /etc/slurm/slurm.conf

# Create the initialization script
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
# Enable User Namespaces for apptainer \n\
sysctl -w kernel.unprivileged_userns_clone=1 || true \n\
# Start SSH service\n\
service ssh start\n\
\n\
# Execute the Slurm startup (not using exec so we can keep the script running)\n\
/etc/startup.sh "$@"\n\
\n\
echo "All services started. Keeping container alive..."\n\
# Keep the container running forever\n\
tail -f /dev/null' > /usr/local/bin/init-ssh-slurm.sh

RUN chmod +x /usr/local/bin/init-ssh-slurm.sh

USER root
ENTRYPOINT ["/usr/local/bin/init-ssh-slurm.sh"]
