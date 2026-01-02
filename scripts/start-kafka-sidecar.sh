#!/bin/bash

# Kafka Sidecar Startup Script
# Port: 38104 (Kafka Sidecar Service)
# This script helps start the kafka sidecar service in development mode
# with proper environment variable detection and instance management.
#
# The Kafka Sidecar bridges Kafka messaging with the Pipestream Engine:
# - Consumes from intake.{datasource_id} and pipestream.{cluster}.{node} topics
# - Hydrates DocumentReferences via Repository Service
# - Hands off hydrated documents to Engine via gRPC
# - Manages topic leases via Consul

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."

# ============================================================================
# Bootstrap Helper Scripts from GitHub (like gradlew)
# ============================================================================

DEV_ASSETS_REPO="https://raw.githubusercontent.com/ai-pipestream/dev-assets/main"
HELPERS_DIR="$PROJECT_ROOT/.dev-helpers"
DEV_ASSETS_LOCATION="${DEV_ASSETS_LOCATION:-$HELPERS_DIR}"

bootstrap_helpers() {
  # Check if DEV_ASSETS_LOCATION is explicitly set by user
  if [ -n "${DEV_ASSETS_LOCATION_OVERRIDE}" ] && [ -f "${DEV_ASSETS_LOCATION_OVERRIDE}/scripts/shared-utils.sh" ]; then
    DEV_ASSETS_LOCATION="${DEV_ASSETS_LOCATION_OVERRIDE}"
    echo "i  Using dev-assets from: $DEV_ASSETS_LOCATION"
    return 0
  fi

  # Check if already bootstrapped
  if [ -f "$HELPERS_DIR/scripts/shared-utils.sh" ]; then
    DEV_ASSETS_LOCATION="$HELPERS_DIR"
    return 0
  fi

  # Bootstrap from GitHub
  echo "Bootstrapping helper scripts from GitHub..."
  mkdir -p "$HELPERS_DIR/scripts"

  if ! curl -fsSL "$DEV_ASSETS_REPO/scripts/shared-utils.sh" -o "$HELPERS_DIR/scripts/shared-utils.sh"; then
    echo "ERROR: Could not download helper scripts from GitHub"
    echo "   Please check your network connection and try again"
    exit 1
  fi

  chmod +x "$HELPERS_DIR/scripts/shared-utils.sh"
  DEV_ASSETS_LOCATION="$HELPERS_DIR"
  echo "Helper scripts downloaded to $HELPERS_DIR"
}

# Bootstrap the helpers
bootstrap_helpers

# Source shared utilities
if [ -f "$DEV_ASSETS_LOCATION/scripts/shared-utils.sh" ]; then
  source "$DEV_ASSETS_LOCATION/scripts/shared-utils.sh"
else
  echo "ERROR: Could not find shared-utils.sh at $DEV_ASSETS_LOCATION/scripts/shared-utils.sh"
  exit 1
fi

# Verify functions are available
if ! type check_dependencies >/dev/null 2>&1; then
  echo "ERROR: Helper functions not loaded properly"
  exit 1
fi

# Service configuration
SERVICE_NAME="Kafka Sidecar"
SERVICE_PORT="38104"
DESCRIPTION="Kafka consumer sidecar that hydrates documents and hands off to engine"

# Check dependencies
check_dependencies "docker" "java"

# Validate we're in the correct directory
validate_project_structure "build.gradle" "src/main/resources/application.properties"

# Set environment variables
export QUARKUS_HTTP_PORT="$SERVICE_PORT"

# Set registration host using Docker bridge detection
set_registration_host "kafka-sidecar" "SIDECAR_SERVICE_HOST"

# Set Consul configuration (can be overridden)
export CONSUL_HOST="${CONSUL_HOST:-localhost}"
export CONSUL_PORT="${CONSUL_PORT:-8500}"

# Set Engine and RepoService connection (sidecar needs these)
export ENGINE_HOST="${ENGINE_HOST:-localhost}"
export ENGINE_PORT="${ENGINE_PORT:-38100}"
export REPO_SERVICE_HOST="${REPO_SERVICE_HOST:-localhost}"
export REPO_SERVICE_PORT="${REPO_SERVICE_PORT:-38102}"

print_status "header" "Starting $SERVICE_NAME"
print_status "info" "Port: $SERVICE_PORT"
print_status "info" "Description: $DESCRIPTION"
print_status "info" "Configuration:"
echo "  Service Host: $SIDECAR_SERVICE_HOST"
echo "  HTTP Port: $QUARKUS_HTTP_PORT"
echo "  Consul: $CONSUL_HOST:$CONSUL_PORT"
echo "  Engine: $ENGINE_HOST:$ENGINE_PORT"
echo "  Repo Service: $REPO_SERVICE_HOST:$REPO_SERVICE_PORT"
echo

# Check if already running and offer to kill
if check_port "$SERVICE_PORT" "$SERVICE_NAME"; then
    print_status "warning" "$SERVICE_NAME is already running on port $SERVICE_PORT."
    read -p "Would you like to kill the existing process and restart? (y/N) " -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        kill_process_on_port "$SERVICE_PORT" "$SERVICE_NAME"
    else
        print_status "info" "Cancelled by user."
        exit 0
    fi
fi

print_status "info" "Starting $SERVICE_NAME in Quarkus dev mode..."
print_status "info" "DevServices will automatically start: Kafka, Consul, etc."
print_status "info" ""
print_status "info" "NOTE: The sidecar requires these services to be running:"
print_status "info" "  - Pipestream Engine (port 38100) - for document handoff"
print_status "info" "  - Repository Service (port 38102) - for document hydration"
print_status "info" "  - Consul (port 8500) - for topic lease management"
print_status "info" "  - Kafka (port 9094) - for message consumption"
print_status "info" ""
print_status "info" "Press Ctrl+C to stop"
echo

# Start using the app's own gradlew
cd "$PROJECT_ROOT"
./gradlew quarkusDev
