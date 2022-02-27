#!/bin/bash

for i in */*.py;
do
  echo Running benchmark ${i}
  cd $(dirname $i)
  python $(basename ${i})
  cd ..
done
