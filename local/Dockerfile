FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

ARG TERRAFORM_VERSION=1.14.2
ARG TERRAGRUNT_VERSION=v0.94.0

RUN apt-get update && apt-get install -y \
    apt-utils \
    git \
    curl \
    jq \
    python3 \
    python3-pip \
    python3-venv \
    unzip \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash dev

RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip \
  && unzip /tmp/awscliv2.zip -d /tmp \
  && /tmp/aws/install \
  && rm -rf /tmp/aws /tmp/awscliv2.zip

RUN curl -fsSL "https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip" \
    -o /tmp/terraform.zip \
  && unzip /tmp/terraform.zip -d /usr/local/bin \
  && rm -f /tmp/terraform.zip

RUN curl -fsSL "https://github.com/gruntwork-io/terragrunt/releases/download/${TERRAGRUNT_VERSION}/terragrunt_linux_amd64" \
    -o /usr/local/bin/terragrunt \
  && chmod +x /usr/local/bin/terragrunt

HEALTHCHECK CMD bash -lc 'command -v aws && command -v terraform && command -v terragrunt && command -v python3 && command -v pip3' || exit 1

USER dev
WORKDIR /workspace
CMD ["/bin/bash"]
