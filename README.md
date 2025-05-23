# Karp-s

Backend for Karp-S

## Installation instructions

1. Create and activate a virtual env
2. Run UV_PROJECT_ENVIRONMENT=$VIRTUAL_ENV
3. Run `make dev`
4. Start development server with `make serve-w-reload`


## Managing dependencies with `uv

Install using `uv sync`.

Add dependencies using `uv add <dep>`, remove using `uv remove <dep>`.

Upgrade existing dependencies using `uv lock --upgrade`.

## Typechecking

This code is type-checked using Pyright, see `pyrightconfig.json` for settings. Pyright is a NPM tool, an example of how to install it:

```
mkdir pyright
cd pyright
npm install pyright
PATH=$PATH:$PWD/pyright/node_modules/.bin/
```

