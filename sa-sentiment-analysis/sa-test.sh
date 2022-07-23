#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"
outputdir="testresults-$($datecmd)"
nproc=$(nproc)

cd $(dirname "$0")
echo "Test started on $(date)"
mkdir -p "$outputdir"

set -x
make -B
for rate in 0; do
    for batching in 0 1 2 4 6 8 16 32 64 128; do
        for pardeg in $(seq 1 $(($nproc / 3))); do
            ./sa --duration="$duration" \
                 --parallelism="$pardeg,$pardeg,$pardeg" \
                 --batch="$batching,$batching" \
                 --chaining=false \
                 --rate="$rate" \
                 --outputdir="$outputdir" \
                 >> "$outputdir/output-$($datecmd).txt"
        done

        for pardeg in $(seq 1 $nproc); do
            ./sa --duration="$duration" \
                 --parallelism="$pardeg,$pardeg,$pardeg" \
                 --batch="$batching",0 \
                 --chaining=true \
                 --rate="$rate" \
                 --outputdir="$outputdir" \
                 >> "$outputdir/output-$($datecmd).txt"
        done
    done
done


echo "Test completed on $(date)"

