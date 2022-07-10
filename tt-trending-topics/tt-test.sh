#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"
outputdir="testresults-$($datecmd)"
nproc=$(nproc)

cd $(dirname "$0")
echo "Test started on $(date)"
mkdir -p "$outputdir"

for rate in 0; do
    for batching in 0 1 2 4 8 16 32 64 128; do
        for pardeg in $(seq 1 $((($nproc / 6) - 3))); do
            set -x
            ./tt-timer-functors \
                --duration="$duration" \
                --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining=false \
                --rate=false \
                --outputdir="$outputdir" >> "$outputdir/output-$($datecmd).txt"
            set +x
        done

        for pardeg in $(seq 1 $((($nproc / 4) - 3))); do
            set -x
            ./tt-timer-functors \
                --duration="$duration" \
                --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining=true \
                --rate=false \
                --outputdir="$outputdir" >> "$outputdir/output-$($datecmd).txt"
            set +x
        done
        
        for pardeg in $(seq 1 $(($nproc / 6))); do
            set -x
            ./tt-timer-threads \
                --duration="$duration" \
                --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining=false \
                --rate=true \
                --outputdir="$outputdir" >> "$outputdir/output-$($datecmd).txt"
            set +x
        done
        
        for pardeg in $(seq $(($nproc / 3))); do
            set +x
            ./tt-timer-threads \
                --duration="$duration" \
                --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                --batch="$batching,$batching,$batching,$batching,$batching" \
                --chaining=true \
                --rate=true \
                --outputdir="$outputdir" >> "$outputdir/output-$($datecmd).txt"
            set +x
        done
    done
done

echo "Test completed on $(date)"

