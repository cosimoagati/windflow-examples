#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"

echo "Test started on $(date)"

for batching in 0 10 100 1000 10000; do
    for chaining in false true; do
        for i in $(seq 1 22); do
            outputdir="testresults-$($datecmd)"
            mkdir -p $outputdir
            set -x
            ./tt-timer-functors \
                --duration="$duration" --parallelism="$i,$i,$i,$i,$i,$i" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining="$chaining" \
                --outputdir="$outputdir" >> "$outputdir/output.txt"
            outputdir="testresults-$($datecmd)"
            mkdir -p $outputdir
            ./tt-timer-threads \
                --duration="$duration" --parallelism="$i,$i,$i" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining="$chaining" \
                --outputdir="$outputdir" >> "$outputdir/output.txt"
            set +x
        done
    done
done

echo "Test completed on $(date)"

