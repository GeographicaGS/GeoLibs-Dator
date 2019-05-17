#!/usr/bin/env bash

set -e

case "$1" in

  jupyter)
    exec jupyter lab --NotebookApp.token= --ip=0.0.0.0 --no-browser --allow-root
  ;;

  lint)
    echo "Linter problems found:"
    exec flake8
  ;;

  test)
    exec pytest --cov
  ;;

esac

exec "$@"
