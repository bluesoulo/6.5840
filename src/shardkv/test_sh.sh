#!/bin/bash
echo "" > error2.log
for i in {1..30}
do
  echo "" >> error2.log
  go test  -race -run TestConcurrent3_5B  >> error2.log
  echo "" >> error2.log
done

