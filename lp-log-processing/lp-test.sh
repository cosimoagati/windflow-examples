#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"
outputdir="testresults-$($datecmd)"
nproc=$(nproc)

cd $(dirname "$0")
echo "Test started on $(date)"
mkdir -p "$outputdir"

set -x
make -j$nproc -B
for rate in 0; do
    for batching in 0 1 2 4 8 16 32 64 128; do
        for pardeg in $(seq 1 $(($nproc / 6))); do
            ./lp --duration="$duration" \
                 --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                 --batch="$batching,$batching,$batching,$batching,$batching" \
                 --chaining=false \
                 --sampling=0 \
                 --rate="$rate" \
                 --outputdir="$outputdir" \
                 >> "$outputdir/output-$($datecmd).txt"
        done
    done
done

echo "Test completed on $(date)"

