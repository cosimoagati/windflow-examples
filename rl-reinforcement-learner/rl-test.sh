#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"
outputdir="testresults-$($datecmd)"

cd $(dirname "$0")
echo "Test started on $(date)"
mkdir -p $outputdir

for rate in 0; do
        for i in $(seq 1 $(expr $(nproc) / 4)); do
    for batching in 0 1 2 4 8 16 32 64 128; do
            set -x
            ./rl --duration="$duration" --parallelism="$i,$i,$i,$i" \
                 --batch="$batching,$batching,$batching" \
                 --chaining="$chaining" \
                 --rate=false \
                 --outputdir="$outputdir" \
                 >> "$outputdir/output-$($datecmd).txt"
            set +x
        done

        for i in $(seq 1 $(nproc)); do
            set -x
            ./rl --duration="$duration" --parallelism="$i,$i,$i,$i" \
                 --batch="$batching,$batching,$batching" \
                 --chaining=true \
                 --rate="$rate" \
                 --outputdir="$outputdir" \
                 >> "$outputdir/output-$($datecmd).txt"
            set +x
        done
    done
done

echo "Test completed on $(date)"

