#!/bin/bash

for i in {1..30}
do
  echo "" >> error2.log
  go test -race  --run 3D  >> error2.log
  echo "" >> error2.log
done

