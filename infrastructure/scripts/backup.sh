
# ============================================================
# infrastructure/scripts/backup.sh - Backup state and configs
# ============================================================

backup() {
    local env=$1
    local backup_dir="${SCRIPT_DIR}/../backups/$(date +%Y%m%d_%H%M%S)"
    
    print_message "$YELLOW" "Creating backup..."
    
    mkdir -p "$backup_dir"
    
    # Backup Terraform state
    cd "${TERRAFORM_DIR}"
    terraform workspace select $env
    terraform state pull > "${backup_dir}/${env}_terraform.tfstate"
    
    # Backup configuration files
    cp -r "${TERRAFORM_DIR}/environments" "${backup_dir}/"
    
    # Create backup manifest
    cat > "${backup_dir}/manifest.json" <<EOF
{
    "environment": "${env}",
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "terraform_version": "$(terraform version -json | jq -r '.terraform_version')",
    "aws_region": "eu-west-2"
}
EOF
    
    print_message "$GREEN" "Backup created at: $backup_dir"
}

# ============================================================
# Main script logic
# ============================================================

# Parse command line arguments
COMMAND=${1:-help}
ENVIRONMENT=${2:-dev}
SKIP_TESTS=${3:-false}
AUTO_APPROVE=${4:-false}

case $COMMAND in
    deploy)
        deploy $ENVIRONMENT $SKIP_TESTS $AUTO_APPROVE
        ;;
    destroy)
        destroy $ENVIRONMENT
        ;;
    validate)
        validate
        ;;
    backup)
        backup $ENVIRONMENT
        ;;
    help|*)
        echo "Usage: $0 {deploy|destroy|validate|backup} [environment] [skip_tests] [auto_approve]"
        echo ""
        echo "Commands:"
        echo "  deploy    - Deploy infrastructure to specified environment"
        echo "  destroy   - Destroy infrastructure in specified environment"
        echo "  validate  - Validate Terraform configuration"
        echo "  backup    - Backup current state and configuration"
        echo ""
        echo "Environments: dev, staging, prod"
        echo ""
        echo "Examples:"
        echo "  $0 deploy dev"
        echo "  $0 deploy staging true    # Skip tests"
        echo "  $0 deploy prod false true  # Auto-approve"
        echo "  $0 destroy dev"
        echo "  $0 validate"
        echo "  $0 backup prod"
        ;;
esac