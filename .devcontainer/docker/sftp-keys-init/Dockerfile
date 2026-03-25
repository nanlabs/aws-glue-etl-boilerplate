FROM alpine:3.19

# Install openssh-keygen (required for SSH key generation)
RUN apk add --no-cache openssh-keygen

# Metadata
LABEL maintainer="NaNLABS"
LABEL description="SFTP Keys generator for local development"
LABEL version="1.0"

WORKDIR /scripts

# Default command (overridden by compose.yml)
CMD ["/bin/sh"]
