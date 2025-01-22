
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
	uv sync

.PHONY: serve-w-reload
serve-w-reload: install-dev
	${INVENV} fastapi dev --port 9000 karps/main.py

.PHONY: lint
lint:
	${INVENV} ruff check .

.PHONY: lint-fix
lint-fix:
	${INVENV} ruff check . --fix

.PHONY: fmt
fmt:
	${INVENV} ruff format .

