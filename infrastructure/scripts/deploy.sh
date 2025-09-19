#!/bin/bash
# ============================================================
# infrastructure/scripts/deploy.sh - Deployment script
# ============================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TERRAFORM_DIR="${SCRIPT_DIR}/../terraform"
LAMBDA_DIR="${SCRIPT_DIR}/../../lambdas"

# Function to print colored output
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to check prerequisites
check_prerequisites() {
    print_message "$YELLOW" "Checking prerequisites..."
    
    # Check for Terraform
    if ! command -v terraform &> /dev/null; then
        print_message "$RED" "Terraform is not installed. Please install Terraform first."
        exit 1
    fi
    
    # Check for AWS CLI
    if ! command -v aws &> /dev/null; then
        print_message "$RED" "AWS CLI is not installed. Please install AWS CLI first."
        exit 1
    fi
    
    # Check for Python
    if ! command -v python3 &> /dev/null; then
        print_message "$RED" "Python 3 is not installed. Please install Python 3 first."
        exit 1
    fi
    
    # Check for zip
    if ! command -v zip &> /dev/null; then
        print_message "$RED" "zip is not installed. Please install zip first."
        exit 1
    fi
    
    print_message "$GREEN" "All prerequisites are installed."
}

# Function to validate environment
validate_environment() {
    local env=$1
    
    if [[ ! "$env" =~ ^(dev|staging|prod)$ ]]; then
        print_message "$RED" "Invalid environment: $env"
        print_message "$YELLOW" "Valid environments are: dev, staging, prod"
        exit 1
    fi
}

# Function to package Lambda functions
package_lambdas() {
    print_message "$YELLOW" "Packaging Lambda functions..."
    
    # Create lambda_packages directory if it doesn't exist
    mkdir -p "${TERRAFORM_DIR}/lambda_packages"
    
    # Package file processor Lambda
    print_message "$YELLOW" "Packaging file_processor Lambda..."
    cd "${LAMBDA_DIR}/file_processor"
    
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt -t ./package --quiet
        cd package
        zip -r ../file_processor.zip . -q
        cd ..
        zip -g file_processor.zip *.py -q
    else
        zip -r file_processor.zip *.py -q
    fi
    
    mv file_processor.zip "${TERRAFORM_DIR}/lambda_packages/"
    
    # Package data validator Lambda
    print_message "$YELLOW" "Packaging data_validator Lambda..."
    cd "${LAMBDA_DIR}/validation"
    
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt -t ./package --quiet
        cd package
        zip -r ../data_validator.zip . -q
        cd ..
        zip -g data_validator.zip *.py -q
    else
        zip -r data_validator.zip *.py -q
    fi
    
    mv data_validator.zip "${TERRAFORM_DIR}/lambda_packages/"
    
    # Clean up package directories
    rm -rf "${LAMBDA_DIR}/file_processor/package"
    rm -rf "${LAMBDA_DIR}/validation/package"
    
    print_message "$GREEN" "Lambda functions packaged successfully."
}

# Function to create Lambda layers
create_layers() {
    print_message "$YELLOW" "Creating Lambda layers..."
    
    mkdir -p "${TERRAFORM_DIR}/layers"
    
    # Create pandas layer
    print_message "$YELLOW" "Creating pandas layer..."
    mkdir -p /tmp/pandas_layer/python
    pip install pandas numpy pyarrow -t /tmp/pandas_layer/python --quiet
    
    cd /tmp/pandas_layer
    zip -r pandas_layer.zip . -q
    mv pandas_layer.zip "${TERRAFORM_DIR}/layers/"
    
    # Clean up
    rm -rf /tmp/pandas_layer
    
    print_message "$GREEN" "Lambda layers created successfully."
}

# Function to initialize Terraform
init_terraform() {
    local env=$1
    
    print_message "$YELLOW" "Initializing Terraform for $env environment..."
    
    cd "${TERRAFORM_DIR}"
    
    # Initialize Terraform with backend config for the environment
    terraform init \
        -backend-config="key=data-platform/${env}/terraform.tfstate" \
        -reconfigure
    
    # Create or select workspace
    terraform workspace select $env 2>/dev/null || terraform workspace new $env
    
    print_message "$GREEN" "Terraform initialized successfully."
}

# Function to plan Terraform deployment
plan_terraform() {
    local env=$1
    
    print_message "$YELLOW" "Planning Terraform deployment for $env environment..."
    
    cd "${TERRAFORM_DIR}"
    
    terraform plan \
        -var-file="environments/${env}.tfvars" \
        -out="${env}.tfplan"
    
    print_message "$GREEN" "Terraform plan created successfully."
}

# Function to apply Terraform deployment
apply_terraform() {
    local env=$1
    local auto_approve=$2
    
    print_message "$YELLOW" "Applying Terraform deployment for $env environment..."
    
    cd "${TERRAFORM_DIR}"
    
    if [ "$auto_approve" == "true" ]; then
        terraform apply "${env}.tfplan"
    else
        terraform apply "${env}.tfplan"
    fi
    
    print_message "$GREEN" "Terraform deployment completed successfully."
}

# Function to deploy Glue scripts
deploy_glue_scripts() {
    local env=$1
    
    print_message "$YELLOW" "Deploying Glue scripts..."
    
    # Get the Glue scripts bucket name from Terraform output
    cd "${TERRAFORM_DIR}"
    GLUE_BUCKET=$(terraform output -raw glue_scripts_bucket 2>/dev/null || echo "ukhsa-glue-scripts-${env}")
    
    # Upload Glue scripts to S3
    aws s3 cp "${SCRIPT_DIR}/../../glue/jobs/" "s3://${GLUE_BUCKET}/scripts/" --recursive
    
    print_message "$GREEN" "Glue scripts deployed successfully."
}

# Function to run post-deployment tests
run_post_deployment_tests() {
    local env=$1
    
    print_message "$YELLOW" "Running post-deployment tests..."
    
    cd "${SCRIPT_DIR}/../.."
    
    # Run smoke tests
    python -m pytest tests/smoke --env=$env -v
    
    print_message "$GREEN" "Post-deployment tests completed successfully."
}

# Main deployment function
deploy() {
    local env=$1
    local skip_tests=${2:-false}
    local auto_approve=${3:-false}
    
    print_message "$GREEN" "Starting deployment to $env environment..."
    
    # Validate environment
    validate_environment $env
    
    # Check prerequisites
    check_prerequisites
    
    # Package Lambda functions
    package_lambdas
    
    # Create Lambda layers
    create_layers
    
    # Initialize Terraform
    init_terraform $env
    
    # Plan Terraform deployment
    plan_terraform $env
    
    # Apply Terraform deployment
    apply_terraform $env $auto_approve
    
    # Deploy Glue scripts
    deploy_glue_scripts $env
    
    # Run post-deployment tests
    if [ "$skip_tests" != "true" ]; then
        run_post_deployment_tests $env
    fi
    
    print_message "$GREEN" "Deployment to $env environment completed successfully!"
}

