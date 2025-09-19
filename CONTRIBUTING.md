
---
# ============================================================
# CONTRIBUTING.md - Contribution guidelines
# ============================================================

# Contributing to UKHSA Data Platform

Thank you for your interest in contributing to the UKHSA Data Platform! This document provides guidelines for contributing to the project.

## Code of Conduct

We are committed to providing a welcoming and inclusive environment. Please read and follow our Code of Conduct.

## How to Contribute

### Reporting Issues

1. Check existing issues to avoid duplicates
2. Use issue templates when available
3. Provide detailed reproduction steps
4. Include environment details (OS, Python version, etc.)

### Submitting Changes

1. **Fork the Repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/ukhsa-data-platform.git
   cd ukhsa-data-platform
   git remote add upstream https://github.com/ukhsa/data-platform.git
   ```

2. **Create a Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make Your Changes**
   - Follow the coding standards
   - Add/update tests
   - Update documentation

4. **Run Tests**
   ```bash
   make test
   make lint
   ```

5. **Commit Your Changes**
   ```bash
   git add .
   git commit -m "feat: add new feature"
   ```
   
   Follow conventional commit format:
   - `feat:` New feature
   - `fix:` Bug fix
   - `docs:` Documentation
   - `style:` Formatting
   - `refactor:` Code restructuring
   - `test:` Adding tests
   - `chore:` Maintenance

6. **Push and Create PR**
   ```bash
   git push origin feature/your-feature-name
   ```

### Pull Request Guidelines

- Fill out the PR template completely
- Link related issues
- Ensure all tests pass
- Request review from CODEOWNERS
- Address review feedback promptly

## Development Setup

```bash
# Install dependencies
make install

# Set up pre-commit hooks
pre-commit install

# Run local environment
docker-compose up -d
```

## Coding Standards

### Python
- Follow PEP 8
- Use type hints
- Document all functions
- Maximum line length: 100 characters

### Terraform
- Use consistent formatting (`terraform fmt`)
- Include descriptions for all variables
- Use meaningful resource names
- Tag all resources appropriately

### Testing
- Write tests for all new features
- Maintain >80% code coverage
- Include both positive and negative test cases
- Use meaningful test names

## Documentation

- Update README for user-facing changes
- Add docstrings to all functions
- Update architecture diagrams if needed
- Include examples in documentation

## Security

- Never commit secrets or credentials
- Use environment variables for configuration
- Follow the principle of least privilege
- Report security vulnerabilities privately

## Questions?

Feel free to ask questions in:
- GitHub Discussions
- Slack: #data-platform-dev
- Email: data-platform@ukhsa.gov.uk

Thank you for contributing to the UKHSA Data Platform!