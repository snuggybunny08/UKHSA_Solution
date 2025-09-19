
# ============================================================
# scripts/setup/install-dependencies.sh
# ============================================================

#!/bin/bash

set -e

echo "Installing UKHSA Data Platform dependencies..."

# Check Python version
if ! python3 --version | grep -q "3.11"; then
    echo "Python 3.11 is required. Please install it first."
    exit 1
fi

# Install Python packages
echo "Installing Python packages..."
pip3 install --upgrade pip
pip3 install -r requirements.txt
pip3 install -r requirements-dev.txt

# Install AWS CLI if not present
if ! command -v aws &> /dev/null; then
    echo "Installing AWS CLI..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
    rm -rf awscliv2.zip aws/
fi

# Install Terraform if not present
if ! command -v terraform &> /dev/null; then
    echo "Installing Terraform..."
    wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
    sudo apt update && sudo apt install terraform
fi

# Install pre-commit hooks
echo "Installing pre-commit hooks..."
pre-commit install

# Install Node.js packages for tools
if [ -f "package.json" ]; then
    echo "Installing Node.js packages..."
    npm install
fi

echo "Dependencies installation complete!"
