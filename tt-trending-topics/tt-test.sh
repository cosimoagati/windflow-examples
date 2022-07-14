#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"
outputdir="testresults-$($datecmd)"
nproc=$(nproc)
timerthreads=3

cd $(dirname "$0")
echo "Test started on $(date)"
mkdir -p "$outputdir/tt-functors" "$outputdir/tt-threads"

set -x
for rate in 0; do
    for freq in 2 4 6 8 10; do
        for batching in 0 1 2 4 8 16 32 64 128; do
            for pardeg in $(seq 1 $((($nproc - $timerthreads) / 6))); do
                ./tt-timer-functors \
                    --duration="$duration" \
                    --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                    --batch="$batching,$batching,$batching,$batching,$batching" \
                    --chaining=false \
                    --frequency="$freq,$freq,$freq" \
                    --rate="$rate" \
                    --outputdir="$outputdir/tt-functors" \
                    >> "$outputdir/tt-functors/output-$($datecmd).txt"
            done

            for pardeg in $(seq 1 $((($nproc - $timerthreads) / 4))); do
                ./tt-timer-functors \
                    --duration="$duration" \
                    --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                    --batch="$batching,$batching,$batching,$batching,$batching" \
                    --chaining=true \
                    --frequency="$freq,$freq,$freq" \
                    --rate="$rate" \
                    --outputdir="$outputdir/tt-functors" \
                    >> "$outputdir/tt-functors/output-$($datecmd).txt"
            done
            
            for pardeg in $(seq 1 $((($nproc - $timerthreads) / 6))); do
                ./tt-timer-threads \
                    --duration="$duration" \
                    --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                    --batch="$batching,$batching,$batching,$batching,$batching" \
                    --chaining=false \
                    --frequency="$freq,$freq,$freq" \
                    --rate="$rate" \
                    --outputdir="$outputdir/tt-threads" \
                    >> "$outputdir/tt-threads/output-$($datecmd).txt"
            done
            
            for pardeg in $(seq $((($nproc - $timerthreads) / 3))); do
                ./tt-timer-threads \
                    --duration="$duration" \
                    --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                    --batch="$batching,$batching,$batching,$batching,$batching" \
                    --chaining=true \
                    --frequency="$freq,$freq,$freq" \
                    --rate="$rate" \
                    --outputdir="$outputdir/tt-threads" \
                    >> "$outputdir/tt-threads/output-$($datecmd).txt"
            done
        done
    done
done

echo "Test completed on $(date)"

