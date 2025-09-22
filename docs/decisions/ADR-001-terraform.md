
# ============================================================
# docs/decisions/ADR-001-terraform.md
# ============================================================

# ADR-001: Use Terraform for Infrastructure as Code

## Status
Accepted

## Context
We need to choose an Infrastructure as Code tool for managing AWS resources for the UKHSA Data Platform.

## Decision
We will use Terraform for infrastructure management.

## Rationale

### Pros:
- Cloud-agnostic (future flexibility)
- Large community and ecosystem
- Mature and stable
- Excellent AWS provider support
- State management capabilities
- Modular architecture support

### Cons:
- Requires learning HCL
- State file management complexity
- Not AWS-native (unlike CloudFormation)

### Alternatives Considered:
1. **AWS CloudFormation**: Native but verbose, limited functionality
2. **AWS CDK**: Good for developers but less operational friendly
3. **Pulumi**: Newer, less mature ecosystem

## Consequences
- Team needs Terraform training
- Need to establish state management strategy
- Must implement proper CI/CD for Terraform
- Benefit from reusable modules
- Easier multi-environment management