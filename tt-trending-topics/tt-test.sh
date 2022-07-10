#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"
outputdir="testresults-$($datecmd)"

cd $(dirname "$0")
echo "Test started on $(date)"
mkdir -p "$outputdir"

for rate in 0; do
    for batching in 0 1 2 4 8 16 32 64 128; do
        for pardeg in $(seq 1 $(expr $(nproc) / 6)); do
            set -x
            ./tt-timer-functors \
                --duration="$duration" \
                --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining="$chaining" \
                --rate=false \
                --outputdir="$outputdir" >> "$outputdir/output-$($datecmd).txt"
            ./tt-timer-threads \
                --duration="$duration" \
                --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining="$chaining" \
                --rate=false \
                --outputdir="$outputdir" >> "$outputdir/output-$($datecmd).txt"
            set +x
        done
        
        for pardeg in $(seq 1 $(nproc)); do
            set -x
            ./tt-timer-functors \
                --duration="$duration" \
                --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining="$chaining" \
                --rate=true \
                --outputdir="$outputdir" >> "$outputdir/output-$($datecmd).txt"
            ./tt-timer-threads \
                --duration="$duration" \
                --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining="$chaining" \
                --rate=true \
                --outputdir="$outputdir" >> "$outputdir/output-$($datecmd).txt"
            set +x
        done
    done
done

echo "Test completed on $(date)"

