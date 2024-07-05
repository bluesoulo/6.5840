#!/bin/bash

for i in {1..10}
do
  echo "" >> error2.log
  go test  -race -run TestConcurrent3_5B >> error2.log
  echo "" >> error2.log
done

