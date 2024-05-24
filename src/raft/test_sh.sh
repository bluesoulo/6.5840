#!/bin/bash

for i in {1..10}
do
  echo "" >> error2.log
  VERBOSE=1 go test -race  --run TestFigure83C  >> error2.log
  echo "" >> error2.log
done

