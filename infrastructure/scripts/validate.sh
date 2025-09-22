
validate() {
    print_message "$YELLOW" "Validating Terraform configuration..."
    
    cd "${TERRAFORM_DIR}"
    
    # Format check
    if ! terraform fmt -check -recursive; then
        print_message "$RED" "Terraform files are not properly formatted. Run 'terraform fmt -recursive' to fix."
        exit 1
    fi
    
    # Validate configuration
    terraform init -backend=false
    terraform validate
    
    print_message "$GREEN" "Terraform configuration is valid."
}
