#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"

cd $(dirname $0)
echo "Test started on $(date)"

for batching in 0 10 100 1000 10000; do
    for chaining in false true; do
        for i in $(seq 1 22); do
            outputdir="testresults-$($datecmd)"
            mkdir $outputdir
            set -x
            ./lp --duration="$duration" --parallelism="$i,$i,$i,$i,$i,$i" \
                 --batch="$batching,$batching,$batching,$batching,$batching" \
                 --chaining="$chaining" \
                 --outputdir="$outputdir" >> "$outputdir/output.txt"
            set +x
        done
    done
done

    echo "Test completed on $(date)"

