#!/bin/bash -eux

export cwd=$(pwd)

pushd $cwd/dp-search-data-importer
  make audit
popd