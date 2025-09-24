#!/bin/bash
if grep -iEq "warning: \[deprecation\]" build.log; then
    echo "=========================================================="
    echo "= Java deprecation warnings detected! Failing the build. ="
    echo "=========================================================="
    grep -iE "warning: \[deprecation\]" -A 2 build.log
    exit 1
else
	echo "No Java deprecation warnings found."
fi
