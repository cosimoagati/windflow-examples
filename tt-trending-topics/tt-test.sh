#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"
outputdir="testresults-$($datecmd)"
nproc=$(nproc)
timerthreads=3

cd $(dirname "$0")
echo Test started on $(date)
mkdir -p "$outputdir"

set -x
make -j$nproc -B
for rate in 0; do
    for freq in 2 4 6 8 10; do
        for batching in 0 1 2 4 8 16 32 64 128; do
            for pardeg in $(seq 1 $((($nproc - $timerthreads) / 6))); do
                for usetimernodes in false true; do
                    ./tt --duration=$duration \
                         --parallelism=$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg \
                         --batch=$batching,$batching,$batching,$batching,$batching \
                         --chaining=false \
                         --frequency=$freq,$freq,$freq \
                         --rate=$rate \
                         --timernodes=$usetimernodes \
                         --outputdir="$outputdir" \
                         >> "$outputdir/output-$($datecmd).txt"
                done
            done

            for pardeg in $(seq 1 $((($nproc - $timerthreads) / 4))); do
                ./tt --duration=$duration \
                     --parallelism=$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg \
                     --batch=$batching,$batching,$batching,$batching,$batching \
                     --chaining=true \
                     --frequency=$freq,$freq,$freq \
                     --rate=$rate \
                     --timernodes=true \
                     --outputdir="$outputdir" \
                     >> "$outputdir/output-$($datecmd).txt"
            done
            
            for pardeg in $(seq $((($nproc - $timerthreads) / 3))); do
                ./tt --duration=$duration \
                     --parallelism=$pardeg,$pardeg,$pardeg,$pardeg,$pardeg,$pardeg \
                     --batch=$batching,$batching,$batching,$batching,$batching \
                     --chaining=true \
                     --frequency=$freq,$freq,$freq \
                     --rate=$rate \
                     --timernodes=false \
                     --outputdir="$outputdir" \
                     >> "$outputdir/output-$($datecmd).txt"
            done
        done
    done
done

echo "Test completed on $(date)"

