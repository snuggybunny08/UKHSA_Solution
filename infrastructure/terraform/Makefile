
.PHONY: help init validate plan apply destroy test clean

ENVIRONMENT ?= dev
SKIP_TESTS ?= false
AUTO_APPROVE ?= false

help:
	@echo "UKHSA Data Platform - Infrastructure Management"
	@echo ""
	@echo "Available targets:"
	@echo "  init       - Initialize Terraform"
	@echo "  validate   - Validate configuration"
	@echo "  plan       - Create deployment plan"
	@echo "  apply      - Apply infrastructure changes"
	@echo "  destroy    - Destroy infrastructure"
	@echo "  test       - Run infrastructure tests"
	@echo "  clean      - Clean temporary files"
	@echo ""
	@echo "Variables:"
	@echo "  ENVIRONMENT  - Target environment (dev|staging|prod)"
	@echo "  SKIP_TESTS   - Skip post-deployment tests (true|false)"
	@echo "  AUTO_APPROVE - Auto-approve Terraform changes (true|false)"

init:
	@echo "Initializing Terraform for $(ENVIRONMENT)..."
	@./scripts/deploy.sh init $(ENVIRONMENT)

validate:
	@echo "Validating configuration..."
	@./scripts/validate.sh

plan:
	@echo "Creating deployment plan for $(ENVIRONMENT)..."
	@./scripts/deploy.sh plan $(ENVIRONMENT)

apply:
	@echo "Applying infrastructure changes to $(ENVIRONMENT)..."
	@./scripts/deploy.sh deploy $(ENVIRONMENT) $(SKIP_TESTS) $(AUTO_APPROVE)

destroy:
	@echo "Destroying infrastructure in $(ENVIRONMENT)..."
	@./scripts/destroy.sh $(ENVIRONMENT)

test:
	@echo "Running infrastructure tests..."
	@cd ../.. && python -m pytest tests/infrastructure -v

clean:
	@echo "Cleaning temporary files..."
	@rm -rf lambda_packages/
	@rm -rf layers/
	@rm -rf *.tfplan
	@rm -rf .terraform/
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete

backup:
	@echo "Creating backup of $(ENVIRONMENT)..."
	@./scripts/backup.sh $(ENVIRONMENT)

lint:
	@echo "Linting Terraform files..."
	@terraform fmt -recursive
	@tflint

security-scan:
	@echo "Running security scan..."
	@tfsec .
	@checkov -d .