#!/bin/bash

for i in {1..10}
do
  echo "" >> error2.log
  VERBOSE4=1 go test -race -run TestPersistPartitionUnreliableLinearizable4A >> error2.log 
  echo "" >> error2.log
done

