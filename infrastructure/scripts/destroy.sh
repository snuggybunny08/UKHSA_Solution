
destroy() {
    local env=$1
    
    print_message "$YELLOW" "WARNING: This will destroy all infrastructure in $env environment!"
    read -p "Are you sure? (yes/no): " confirmation
    
    if [ "$confirmation" != "yes" ]; then
        print_message "$YELLOW" "Destruction cancelled."
        exit 0
    fi
    
    cd "${TERRAFORM_DIR}"
    
    terraform workspace select $env
    terraform destroy -var-file="environments/${env}.tfvars" -auto-approve
    
    print_message "$GREEN" "Infrastructure destroyed successfully."
}