#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"

echo "Test started on $(date)"

for chaining in false true; do
    for i in $(seq 1 22); do
        ./sa --duration="$duration" --parallelism="$i,$i,$i" \
             --chaining="$chaining"  --outputdir="testresults-$($datecmd)"
    done
done

echo "Test completed on $(date)"

