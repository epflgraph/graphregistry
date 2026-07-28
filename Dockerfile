# syntax=docker/dockerfile:1.7

# Use a specific version of the Python 3.11 slim image for better reproducibility
FROM python:3.11.13-slim-bookworm@sha256:86adf8dbadc3d6e82ee5dd2c74bec2e1c2467cdad47886280501df722372d2e1

# Set environment variables to optimize Python behavior and pip usage
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    GRAPHREGISTRY_ROOT=/app \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Install git for potential use in the application and clean up apt cache to reduce image size
WORKDIR /app

# Install git and clean up apt cache to reduce image size
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install dependencies
COPY docker/requirements.txt /tmp/requirements.txt

# Use pip's cache mount to speed up subsequent builds by caching downloaded packages
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /tmp/requirements.txt

# Copy packaging metadata and the application code into the container
COPY pyproject.toml README.md ./
COPY graphregistry ./graphregistry
COPY database ./database
COPY docker ./docker

# Install the graphregistry package itself in editable mode so that package
# metadata (including the version) is available to importlib.metadata.
# --no-deps is used because all runtime dependencies were already installed
# from docker/requirements.txt; without it, any change to the source code would
# invalidate this layer and re-run the full dependency resolution.
RUN pip install --no-deps --no-cache-dir -e .

# Create a directory for configuration files and ensure the entrypoint script is executable.
# Runtime configs (including config_api.json) are mounted at deploy time, not baked into the image.
RUN mkdir -p /app/config \
    && chmod +x /app/docker/entrypoint.sh

# Expose the application port
EXPOSE 28800

# Set the entrypoint to the script that will start the application
ENTRYPOINT ["/app/docker/entrypoint.sh"]
