#!/bin/bash -eux

pushd dp-search-data-importer
  make build
  cp build/dp-search-data-importer Dockerfile.concourse ../build
popd
