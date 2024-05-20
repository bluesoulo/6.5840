#!/bin/bash

for i in {1..100}
do
  go test -race --run  TestReElection3A >> error.log
done

