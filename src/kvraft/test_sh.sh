#!/bin/bash
echo "" > error2.log
for i in {1..10}
do
  echo "" >> error2.log
  go test -race -run 4A >> error2.log
  echo "" >> error2.log
done

