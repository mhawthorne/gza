# Docker Testing Environment

A minimal Docker image for testing fresh installs and running demos in an isolated environment.

## Build the Image

```bash
docker build -t gza-test -f etc/Dockerfile.test .
```

## Run Interactively

```bash
docker run -it -e ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" gza-test
```

Or with an env file:

```bash
docker run -it --env-file ~/.env gza-test
```

## Install gza Inside the Container

From PyPI (production):

```bash
uv pip install --system gza
```

From TestPyPI:

```bash
uv pip install --system --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ gza
```

The `--extra-index-url` is needed because TestPyPI won't have dependencies like `pyyaml`.

## Verify Installation

```bash
gza --help
```
