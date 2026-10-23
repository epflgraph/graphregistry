#!/usr/bin/env bash

while ./scripts/run2.sh || [[ $? -ne 130 ]]; do
  :
done
