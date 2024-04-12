#!/bin/bash

set -exv

IMAGE="quay.io/cloudservices/xjoin-validation"
IMAGE_TAG=$(git rev-parse --short=7 HEAD)
SMOKE_TEST_TAG="latest"

if [[ -z "$QUAY_USER" || -z "$QUAY_TOKEN" ]]; then
    echo "QUAY_USER and QUAY_TOKEN must be set"
    exit 1
fi


#AUTH_CONF_DIR="$(pwd)/.podman"
#mkdir -p $AUTH_CONF_DIR
#export REGISTRY_AUTH_FILE="$AUTH_CONF_DIR/auth.json"

#podman login -u="$QUAY_USER" -p="$QUAY_TOKEN" quay.io
#podman login -u="$RH_REGISTRY_USER" -p="$RH_REGISTRY_TOKEN" registry.redhat.io
#podman build --pull=true -f Dockerfile -t "${IMAGE}:${IMAGE_TAG}" .
#podman push "${IMAGE}:${IMAGE_TAG}"

# To enable backwards compatibility with ci, qa, and smoke, always push latest and qa tags
#podman tag "${IMAGE}:${IMAGE_TAG}" "${IMAGE}:latest"
#podman push "${IMAGE}:latest"
#podman tag "${IMAGE}:${IMAGE_TAG}" "${IMAGE}:qa"
#podman push "${IMAGE}:qa"

# Create tmp dir to store data in during job run (do NOT store in $WORKSPACE)
export TMP_JOB_DIR=$(mktemp -d -p "$HOME" -t "jenkins-${JOB_NAME}-${BUILD_NUMBER}-XXXXXX")
echo "job tmp dir location: $TMP_JOB_DIR"

function job_cleanup() {
    echo "cleaning up job tmp dir: $TMP_JOB_DIR"
    rm -fr $TMP_JOB_DIR
}

trap job_cleanup EXIT ERR SIGINT SIGTERM

DOCKER_CONF="$TMP_JOB_DIR/.docker"
mkdir -p "$DOCKER_CONF"
docker --config="$DOCKER_CONF" login -u="$QUAY_USER" -p="$QUAY_TOKEN" quay.io
docker --config="$DOCKER_CONF" build -t "${IMAGE}:${IMAGE_TAG}" .
docker --config="$DOCKER_CONF" tag "${IMAGE}:${IMAGE_TAG}" "${IMAGE}:latest"
docker --config="$DOCKER_CONF" push "${IMAGE}:${IMAGE_TAG}"
docker --config="$DOCKER_CONF" push "${IMAGE}:latest"
