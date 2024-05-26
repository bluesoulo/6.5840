#!/bin/bash

for i in {1..30}
do
  echo "" >> error2.log
  VERBOSE=1 go test -race  --run TestFigure8Unreliable3C  >> error2.log
  echo "" >> error2.log
done

