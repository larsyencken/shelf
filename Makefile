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
	rye sync

# Run unittests with pytest
unittest: .venv
	pytest

# Reformat using rye
format: .venv
	rye format

# Lint using rye
lint: .venv
	rye lint

# Typecheck with pyright
typecheck: .venv
	pyright

# Run lint, typecheck, and unittest sequentially
test: lint typecheck unittest
