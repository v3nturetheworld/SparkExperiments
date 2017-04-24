#!/bin/bash

echo -e "If this is the first time running, it's going to hang for a few seconds while Maven sets things up.\nCompiling program..."

sbt clean package

echo "Done Building. Please use run scripts"
