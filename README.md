# Karp-s

Backend for Karp-s

## Installation instructions

One way to do it, there are many! For example `uv` can handle both
Python versions and virtual environments completely. In this example
only Python is pre-installed and uv is installed in a virtual environment.

1. Create and activate a virtual env
2. Run `UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV`
3. Run `make dev`
4. Start development server with `make serve-w-reload`

If `UV_PROJECT_ENVIRONEMNT` is not set, but `uv` is available, it will default
to creating a venv in .venv and all commands will work as expected.

## Running the backend

The backend is run using `make serve`. It defaults to running one worker on port
9000. It is possible to use environment variables to change the number of workers
and port:

`PORT=8000 NUM_WORKERS=10 make serve` 

Running a backend that reloads on code changes are done using `make serve-w-reload`

## Caching

The workers only read the configuration files on startup. To serve new code or 
configuration use `make reload`.

## Managing dependencies with `uv

Install using `uv sync`.

Add dependencies using `uv add [--dev] <dep>`, remove using `uv remove <dep>`.

Upgrade existing dependencies using `uv lock --upgrade`.

## Typechecking

This code is type-checked using basedpyright, see `pyrightconfig.json` for settings.
basedpyright is installed in the venv and is used instead of Pyright because
Pyright requires NodeJS. There is also a language server for basedpyright that
can be used in your editor for completions and highlighting.

