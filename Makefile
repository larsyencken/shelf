.PHONY: help unittest format lint typecheck test

# Default target
help: 
	@echo "Available targets:"
	@echo "  make unittest  - Run unittests with pytest"
	@echo "  make format    - Reformat using rye"
	@echo "  make lint      - Lint using rye"
	@echo "  make typecheck - Typecheck with pyright"
	@echo "  make test      - Run lint, typecheck, and unittest sequentially"

# Check if .venv exists and is up to date
.venv: pyproject.toml
	@echo "==> Installing packages"
	@rye sync

# Run unittests with pytest
unittest: .venv
	@echo "==> Running unit tests"
	@rye test

# Reformat using rye
format: .venv
	@echo "==> Formatting all files"
	@rye format

# Lint using rye
lint: .venv
	@echo "==> Linting all files"
	@rye lint

# Typecheck with pyright
typecheck: .venv
	@echo "==> Typechecking"
	@rye run pyright

# Run lint, typecheck, and unittest sequentially
test: lint typecheck unittest
