# Use the linuxserver/openssh-server as the base (Alpine-based)
FROM linuxserver/openssh-server:latest

# 1. Replace http with https in the repository file
# 2. Add the community repository (using https)
# 3. Update and install OpenJDK 21
RUN sed -i 's|http://|https://|g' /etc/apk/repositories && \
    echo "https://dl-cdn.alpinelinux.org/alpine/v3.23/community" >> /etc/apk/repositories && \
    apk update && \
    apk add --no-cache openjdk21

# Set environment variables for Java (Optional but recommended)
ENV JAVA_HOME=/usr/lib/jvm/default-jvm
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Create /scratch/ folder with write rights for everyone
RUN mkdir -p /scratch && chmod 777 /scratch

# Verify the installation during the build process
RUN java -version
