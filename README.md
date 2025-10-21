# Karp-s

Backend for Karp-s

## Installation instructions

One way to do it, there are many! For example `uv` can handle both
Python versions and virtual environments completely. In this example
only Python is pre-installed and uv is installed in a virtual environment.

1. Create and activate a virtual env
2. Run UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV
3. Run `make dev`
4. Start development server with `make serve-w-reload`

## Managing dependencies with `uv

Install using `uv sync`.

Add dependencies using `uv add [--dev] <dep>`, remove using `uv remove <dep>`.

Upgrade existing dependencies using `uv lock --upgrade`.

## Typechecking

This code is type-checked using basedpyright, see `pyrightconfig.json` for settings.
basedpyright is installed in the venv and is used instead of Pyright because
Pyright requires NodeJS. There is also a language server for basedpyright that
can be used in your editor for completions and highlighting.

