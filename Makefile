
.PHONY: help
help:
	@echo "usage:"
	@echo ""
	@echo "serve-w-reload"
	@echo "   start web API with automatic reload"
	@echo ""
	@echo "dev | install-dev"
	@echo "   setup development environment"
	@echo ""
	@echo "fmt"
	@echo "   run formatter on all code"
	@echo ""
	@echo "lint"
	@echo "   run lint on all code"
	@echo ""
	@echo "lint-fix"
	@echo "   run lint on all code and fix fixable things"
	@echo ""

dev: install-dev
install-dev:
	pip install --upgrade pip
	pip install uv
	uv sync

.PHONY: serve-w-reload
serve-w-reload: install-dev
	fastapi dev --port 9000 karps/api.py

.PHONY: test
test:
	PYTHONPATH=. pytest

.PHONY: lint
lint:
	ruff check .

.PHONY: lint-fix
lint-fix:
	ruff check . --fix

.PHONY: fmt
fmt:
	ruff format .

