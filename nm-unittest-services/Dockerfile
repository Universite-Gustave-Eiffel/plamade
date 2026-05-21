FROM nathanhess/slurm:base

USER root

# 1. Install packages and force system shell to Bash
RUN apt-get update && apt-get install -y \
    openssh-server unzip rsync sudo software-properties-common curl sed \
    && add-apt-repository -y ppa:apptainer/ppa \
    && apt-get update && apt-get install -y \
    environment-modules \
    apptainer \
    && echo "dash dash/sh boolean false" | debconf-set-selections \
    && DEBIAN_FRONTEND=noninteractive dpkg-reconfigure dash \
    && rm -f /etc/profile.d/modules.sh /usr/share/modules/init/bash \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN sed -i 's/^allow setuid =.*/allow setuid = yes/' /etc/apptainer/apptainer.conf && \
    sed -i 's/^enable overlay =.*/enable overlay = no/' /etc/apptainer/apptainer.conf && \
    sed -i 's/^mount dev =.*/mount dev = yes/' /etc/apptainer/apptainer.conf && \
    sed -i 's/^bind path =.*/bind path = /' /etc/apptainer/apptainer.conf

# 2. HPC Simulation Logic
RUN rm -f /usr/bin/singularity && \
    mkdir -p /usr/local/bin && chmod 777 /usr/local/bin

# 3. Create 'module' command
# It creates a symlink in /usr/local/bin, which we know is in your SSH PATH.
RUN printf '#!/bin/bash\n\
case "$1" in\n\
  load)\n\
    if [[ "$2" == *"singularity"* ]]; then\n\
        ln -sf /usr/bin/apptainer /usr/local/bin/singularity\n\
        echo "Module $2 loaded (emulated)"\n\
    else\n\
        echo "ERROR: Unable to locate a modulefile for $2"\n\
        exit 1\n\
    fi ;;\n\
  unload)\n\
    rm -f /usr/local/bin/singularity ;;\n\
  *)\n\
    echo "Module command emulated for singularity" ;;\n\
esac\n' > /usr/local/bin/module && chmod +x /usr/local/bin/module

# 4. SSH & Slurm Configuration
RUN mkdir -p /var/run/sshd /scratch && chmod 777 /scratch \
    && echo "PermitUserEnvironment yes" >> /etc/ssh/sshd_config \
    && sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config \
    && printf '#!/bin/bash\nrm -rf /scratch/job.${SLURM_JOB_ID}\n' > /etc/slurm/epilog.sh \
    && chmod +x /etc/slurm/epilog.sh \
    && echo "Epilog=/etc/slurm/epilog.sh" >> /etc/slurm/slurm.conf

COPY prolog.sh /etc/slurm/prolog.sh
RUN chmod +x /etc/slurm/prolog.sh && echo "Prolog=/etc/slurm/prolog.sh" >> /etc/slurm/slurm.conf

# 5. Initialization Script
RUN printf '#!/bin/bash\n\
USERNAME=${USER_NAME:-testuser}\n\
USER_HOME=/home/$USERNAME\n\
if ! id -u "$USERNAME" >/dev/null 2>&1; then\n\
    useradd -m -s /bin/bash "$USERNAME"\n\
    mkdir -p "$USER_HOME/.ssh"\n\
    chown -R "$USERNAME:$USERNAME" "$USER_HOME/.ssh"\n\
fi\n\
if [ -n "$PUBLIC_KEY" ]; then\n\
    echo "$PUBLIC_KEY" > "$USER_HOME/.ssh/authorized_keys"\n\
    chown "$USERNAME:$USERNAME" "$USER_HOME/.ssh/authorized_keys"\n\
    chmod 600 "$USER_HOME/.ssh/authorized_keys"\n\
fi\n\
[ "$SUDO_ACCESS" = "true" ] && echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/$USERNAME\n\
sysctl -w kernel.unprivileged_userns_clone=1 || true\n\
service ssh start\n\
/etc/startup.sh "$@"\n\
tail -f /dev/null\n' > /usr/local/bin/init-ssh-slurm.sh && chmod +x /usr/local/bin/init-ssh-slurm.sh

RUN sed -i '/APPTAINER_BIND/d' /usr/local/bin/init-ssh-slurm.sh || true
RUN sed -i '/APPTAINER_BINDPATH/d' /usr/local/bin/init-ssh-slurm.sh || true

ENTRYPOINT ["/usr/local/bin/init-ssh-slurm.sh"]
