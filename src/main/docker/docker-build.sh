#!/bin/bash

# Ensure the bundled jar is available in the Docker context.
# shellcheck disable=SC2154
cp "../${project.artifactId}-bundled-${project.version}.jar" .
docker build -f Dockerfile -t "${docker.registry}db-to-avro:${project.version}-${project.build}" ../../../
