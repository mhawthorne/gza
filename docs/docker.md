# Docker Configuration

Gza runs AI providers (Claude, Gemini) in Docker containers by default for isolation and reproducibility. This document explains how to customize the Docker environment for your project's needs.

## Default Behavior

When you first run `gza work`, it automatically:

1. Generates a minimal Dockerfile at `etc/Dockerfile.claude` (or `etc/Dockerfile.gemini`)
2. Builds a Docker image named `{project_name}-gza`
3. Runs the AI provider inside that container

The default Dockerfile only includes Node.js and the AI CLI tool:

```dockerfile
FROM node:20-slim

RUN npm install -g @anthropic-ai/claude-code

RUN useradd -m -s /bin/bash gza
USER gza
WORKDIR /home/gza

CMD ["claude"]
```

## Custom Dockerfiles

For projects that need additional tools (Python, compilers, test frameworks, etc.), you can provide a custom Dockerfile.

### Creating a Custom Dockerfile

1. Create or edit `etc/Dockerfile.claude` (or `etc/Dockerfile.gemini` for Gemini)
2. Add your project's dependencies
3. The image will be rebuilt automatically on the next `gza work`

### Example: Python Project

```dockerfile
FROM node:20-slim

# Install Python and common build tools
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv (fast Python package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Claude Code CLI
RUN npm install -g @anthropic-ai/claude-code

# Create gza user for isolation
RUN useradd -m -s /bin/bash gza

# Make uv available to gza user
RUN cp /root/.local/bin/uv /usr/local/bin/uv

USER gza
WORKDIR /home/gza

CMD ["claude"]
```

### Example: Rust Project

```dockerfile
FROM node:20-slim

# Install Rust
RUN apt-get update && apt-get install -y curl build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Install Claude Code CLI
RUN npm install -g @anthropic-ai/claude-code

RUN useradd -m -s /bin/bash gza
ENV PATH="/root/.cargo/bin:$PATH"
USER gza
WORKDIR /home/gza

CMD ["claude"]
```

### Example: Go Project

```dockerfile
FROM node:20-slim

# Install Go
RUN apt-get update && apt-get install -y wget \
    && rm -rf /var/lib/apt/lists/*
RUN wget -q https://go.dev/dl/go1.22.0.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz \
    && rm go1.22.0.linux-amd64.tar.gz

ENV PATH="/usr/local/go/bin:$PATH"

# Install Claude Code CLI
RUN npm install -g @anthropic-ai/claude-code

RUN useradd -m -s /bin/bash gza
USER gza
WORKDIR /home/gza

CMD ["claude"]
```

## Automatic Rebuild

Gza automatically detects when your Dockerfile has changed:

- Compares the Dockerfile's modification time against the Docker image's creation time
- Rebuilds the image if the Dockerfile is newer
- Prints "Dockerfile changed, rebuilding..." when this happens

To force a rebuild manually:

```bash
docker rmi {project_name}-gza
gza work
```

## Disabling Docker

To run without Docker (using locally installed CLI tools):

```bash
# One-time
gza work --no-docker

# Permanently in gza.yaml
use_docker: false
```

## File Locations

| File | Purpose |
|------|---------|
| `etc/Dockerfile.claude` | Custom Dockerfile for Claude provider |
| `etc/Dockerfile.gemini` | Custom Dockerfile for Gemini provider |

## Troubleshooting

### "python3: No such file or directory"

The default container doesn't include Python. Create a custom Dockerfile that installs Python (see examples above).

### "Permission denied" errors

The container runs as the `gza` user for isolation. Make sure any installed tools are accessible to this user (copy binaries to `/usr/local/bin/` or add to PATH).

### Build cache issues

Docker caches layers. To rebuild from scratch:

```bash
docker rmi {project_name}-gza
docker builder prune
gza work
```

### macOS file sharing

On macOS, Docker needs access to the directories gza uses. The default `/tmp/gza-worktrees` path is accessible. If you change `worktree_dir` in `gza.yaml`, ensure Docker can access it via Docker Desktop > Settings > Resources > File Sharing.
