#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"
outputdir="testresults-$($datecmd)"

cd $(dirname $0)
echo "Test started on $(date)"
mkdir -p "$outputdir"

for batching in 0 10 100 1000 10000; do
    for chaining in false true; do
        for i in $(seq 1 22); do
            set -x
            ./mo --duration="$duration" --parallelism="$i,$i,$i,$i,$i" \
                 --batch="$batching,$batching,$batching,$batching" \
                 --chaining="$chaining" \
                 --outputdir="$outputdir" \
                 >> "$outputdir/output-$($datecmd).txt"
            set +x
        done
    done
done

echo "Test completed on $(date)"

