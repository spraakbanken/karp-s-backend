
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
	@echo "   run formatter and lint with autofix on all code"
	@echo ""
	@echo "check-types"
	@echo "   run typechecker on code (needs Pyright, see README.md)"

dev: install-dev
install-dev:
	pip install --upgrade pip
	pip install uv
	uv sync

.PHONY: serve-w-reload
serve-w-reload: install-dev
	fastapi dev --port 9000 karps/api.py

.PHONY: serve
serve: install-dev
	python -m uvicorn karps.api:app --port 9000

.PHONY: test
test:
	PYTHONPATH=. pytest

.PHONY: update-snapshots
update-snapshots:
	PYTHONPATH=. pytest --snapshot-update

.PHONY: fmt
fmt:
	ruff format .
	ruff check . --fix

.PHONY: check-types
check-types:
	basedpyright
