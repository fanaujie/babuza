#!/bin/bash
# build-docker.sh - Script to build babuza-kvstore Docker image

set -e

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KVSTORE_DIR="$(dirname "$SCRIPT_DIR")"
BABUZA_ROOT="$(dirname "$(dirname "$KVSTORE_DIR")")"

echo "🚀 Building babuza-kvstore Docker image..."
echo "- Script directory: $SCRIPT_DIR"
echo "- KVStore directory: $KVSTORE_DIR"
echo "- Babuza root directory: $BABUZA_ROOT"

# Create a temporary build directory
BUILD_DIR="$SCRIPT_DIR/build_tmp"
echo "- Creating temporary build directory: $BUILD_DIR"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Copy the kvstore application files (excluding docker directory to avoid recursive copy)
echo "- Copying KVStore application files..."
for item in "$KVSTORE_DIR"/*; do
    if [ "$(basename "$item")" != "docker" ]; then
        cp -r "$item" "$BUILD_DIR/"
    fi
done

# Create directories for local babuza modules
mkdir -p "$BUILD_DIR/babuza_modules/ibabuza"
mkdir -p "$BUILD_DIR/babuza_modules/pkg"
mkdir -p "$BUILD_DIR/babuza_modules/raft"

# Copy babuza modules to the build directory
echo "- Copying babuza modules..."
cp -r "$BABUZA_ROOT/ibabuza"/* "$BUILD_DIR/babuza_modules/ibabuza/"
cp -r "$BABUZA_ROOT/pkg"/* "$BUILD_DIR/babuza_modules/pkg/"
cp -r "$BABUZA_ROOT/raft"/* "$BUILD_DIR/babuza_modules/raft/"

# Modify go.mod to use local replace directives
echo "- Updating go.mod with replace directives..."
# Copy original go.mod
cp "$KVSTORE_DIR/go.mod" "$BUILD_DIR/go.mod"
# Append replace directives
cat >> "$BUILD_DIR/go.mod" << EOF

// Local replace directives for babuza modules
replace (
	github.com/fanaujie/babuza/ibabuza => ./babuza_modules/ibabuza
	github.com/fanaujie/babuza/pkg => ./babuza_modules/pkg
	github.com/fanaujie/babuza/raft => ./babuza_modules/raft
)
EOF

# Create a modified Dockerfile in the build directory
echo "- Creating modified Dockerfile for the build..."
cat > "$BUILD_DIR/Dockerfile" << EOF
FROM golang:1.24-alpine AS builder

WORKDIR /app
RUN apk --no-cache add git

# Copy all files including babuza modules
COPY . .

# Run go mod tidy to ensure dependencies are correct
RUN go mod tidy

# Build the application
RUN go build -o kvstore .

# Use a small alpine image for the final container
FROM alpine:latest

WORKDIR /app
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/kvstore /app/kvstore

RUN mkdir -p /data/raft_storage
RUN adduser -D -h /app kvstore
VOLUME /data
EXPOSE 14200 24200

ENTRYPOINT ["/app/kvstore"]
EOF

# Build the Docker image
echo "- Building Docker image..."
cd "$BUILD_DIR"
docker build -t babuza-kvstore:latest .

# Clean up
echo "- Cleaning up temporary build directory..."
cd "$SCRIPT_DIR"
rm -rf "$BUILD_DIR"

echo "✅ Docker image babuza-kvstore:latest successfully built!"