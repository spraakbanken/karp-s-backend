# setup uv
UV_EXISTS := $(shell command -v uv 2>/dev/null)

# if a venv is active, use --active to ignore any mismatch between current venv and uv project environment path
ifneq (${VIRTUAL_ENV},)
ACTIVE := --active
endif

UV = uv run $(ACTIVE)

.PHONY: help
help:
	@echo "usage:"
	@echo ""
	@echo "serve-w-reload"
	@echo "   start web API with automatic reload"
	@echo ""
	@echo "serve"
	@echo "   start web API without automatic reload"
	@echo ""
	@echo "dev | install-dev"
	@echo "   setup development environment"
	@echo ""
	@echo "fmt"
	@echo "   run formatter and lint with autofix on all code"
	@echo ""
	@echo "check-types"
	@echo "   run typechecker on code"


.PHONY: dev install-dev
dev: .installed
install-dev: .installed


.installed: uv.lock pyproject.toml
ifeq ($(UV_EXISTS),)
	# uv does not exist
	ifeq (${VIRTUAL_ENV},)
		# no virtual env exists
		@echo "Set up either uv or a virtual environment to install with this Makefile. See README.md"
		@false
	else
		# if venv is active but uv is not found, install uv
		pip install uv
	endif
else
	@:
endif
	# use $(ACTIVE) to use the active venv
	uv sync $(ACTIVE) --group prod
	@touch $@

run:
	mkdir run

.PHONY: serve serve-w-reload

PORT ?= 9000
NUM_WORKERS ?= 1

GUNICORN_BASE = $(UV) gunicorn karps.api:app --control-socket run/gunicorn.ctl --worker-class asgi --workers $(NUM_WORKERS) --bind 127.0.0.1:$(PORT) --pid run/gunicorn.pid

serve: install-dev run
	$(GUNICORN_BASE)

serve-w-reload: install-dev run
	$(GUNICORN_BASE) --reload --graceful-timeout 1

.PHONY: reload
reload: run/gunicorn.ctl
	$(UV) gunicornc -s run/gunicorn.ctl -c "reload"

run/gunicorn.ctl:
	@echo "Cannot find gunicorn control socket, have you run make serve(-w-reload)?"
	@exit 1

.PHONY: test
test:
	PYTHONPATH=. $(UV) pytest $(ARGS)

.PHONY: update-snapshots
update-snapshots:
	PYTHONPATH=. $(UV) pytest --snapshot-update $(ARGS)

.PHONY: fmt
fmt:
	$(UV) ruff format .
	$(UV) ruff check . --fix

.PHONY: check-types
check-types:
	$(UV) basedpyright
