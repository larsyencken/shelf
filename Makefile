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
	@uv sync
	@touch $@

# Run unittests with pytest
unittest: .venv
	@echo "==> Running unit tests"
	@rye test -- --sw

# Reformat using rye
format: .venv
	@echo "==> Formatting all files"
	@uv run ruff format
	@uv run ruff check --fix

# Lint using rye
lint: .venv
	@echo "==> Linting all files"
	@uv run ruff check

# Typecheck with pyright
typecheck: .venv
	@echo "==> Typechecking"
	@uv run pyright

# Run lint, typecheck, and unittest sequentially
test: lint typecheck unittest

clean:
	rm -rf data/* metadata/*
