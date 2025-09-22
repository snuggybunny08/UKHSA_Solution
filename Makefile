
.PHONY: help install test deploy destroy clean lint format

# Default environment
ENV ?= dev
REGION ?= eu-west-2
PROFILE ?= ukhsa-$(ENV)

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(GREEN)UKHSA Data Platform - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

install: ## Install all dependencies
	@echo "$(GREEN)Installing dependencies...$(NC)"
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pre-commit install
	@echo "$(GREEN)Dependencies installed successfully!$(NC)"

test: ## Run all tests
	@echo "$(GREEN)Running tests...$(NC)"
	pytest tests/ -v --cov=lambdas --cov=glue --cov-report=html --cov-report=term

test-unit: ## Run unit tests only
	pytest tests/unit -v

test-integration: ## Run integration tests
	pytest tests/integration -v

test-smoke: ## Run smoke tests against deployed environment
	pytest tests/smoke -v --env=$(ENV)

lint: ## Run linting checks
	@echo "$(GREEN)Running linters...$(NC)"
	flake8 lambdas/ glue/ tests/
	pylint lambdas/ glue/
	black --check lambdas/ glue/ tests/
	cd infrastructure/terraform && terraform fmt -check -recursive

format: ## Format code
	@echo "$(GREEN)Formatting code...$(NC)"
	black lambdas/ glue/ tests/
	cd infrastructure/terraform && terraform fmt -recursive

security-scan: ## Run security scans
	@echo "$(GREEN)Running security scans...$(NC)"
	safety check
	bandit -r lambdas/ glue/
	cd infrastructure/terraform && tfsec .

terraform-init: ## Initialize Terraform
	cd infrastructure/terraform && terraform init

terraform-plan: ## Plan Terraform changes
	cd infrastructure/terraform && terraform plan -var-file=environments/$(ENV).tfvars

terraform-apply: ## Apply Terraform changes
	cd infrastructure/terraform && terraform apply -var-file=environments/$(ENV).tfvars

deploy: ## Deploy to specified environment
	@echo "$(GREEN)Deploying to $(ENV) environment...$(NC)"
	cd infrastructure && ./scripts/deploy.sh deploy $(ENV)

destroy: ## Destroy infrastructure in specified environment
	@echo "$(RED)WARNING: Destroying $(ENV) environment...$(NC)"
	cd infrastructure && ./scripts/destroy.sh $(ENV)

package-lambdas: ## Package Lambda functions
	@echo "$(GREEN)Packaging Lambda functions...$(NC)"
	cd infrastructure && ./scripts/package-lambdas.sh

upload-glue: ## Upload Glue scripts to S3
	@echo "$(GREEN)Uploading Glue scripts...$(NC)"
	aws s3 sync glue/jobs/ s3://ukhsa-glue-scripts-$(ENV)/scripts/ --profile $(PROFILE)

logs: ## Tail CloudWatch logs
	aws logs tail /aws/lambda/ukhsa-file-processor-$(ENV) --follow --profile $(PROFILE)

dashboard: ## Open CloudWatch dashboard
	@echo "$(GREEN)Opening CloudWatch dashboard...$(NC)"
	open "https://$(REGION).console.aws.amazon.com/cloudwatch/home?region=$(REGION)#dashboards:name=ukhsa-$(ENV)-dashboard"

clean: ## Clean build artifacts
	@echo "$(YELLOW)Cleaning build artifacts...$(NC)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache htmlcov .coverage
	rm -rf lambda_packages/ layers/ *.zip
	@echo "$(GREEN)Clean complete!$(NC)"

validate: ## Validate all configurations
	@echo "$(GREEN)Validating configurations...$(NC)"
	cd infrastructure && ./scripts/validate.sh
	yamllint .github/workflows/
	jsonlint config/**/*.json

docs: ## Build documentation
	@echo "$(GREEN)Building documentation...$(NC)"
	cd docs && mkdocs build

serve-docs: ## Serve documentation locally
	cd docs && mkdocs serve

backup: ## Backup current state
	@echo "$(GREEN)Creating backup...$(NC)"
	cd infrastructure && ./scripts/backup.sh $(ENV)