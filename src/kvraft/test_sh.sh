#!/bin/bash
echo "" > error2.log
for i in {1..5}
do
  echo "" >> error2.log
  go test -race -run TestManyPartitionsOneClient4A  >> error2.log
  echo "" >> error2.log
done

