#!/bin/sh

duration=120
datecmd="date +%Y-%m-%d-%H-%M"
outputdir="testresults-$($datecmd)"
nproc=$(nproc)

cd $(dirname "$0")
echo "Test started on $(date)"
mkdir -p "$outputdir"

set -x
for rate in 0; do
    for execmode in deterministic default; do
        for anomaly_scorer in data-stream sliding-window; do
            for alert_triggerer in top-k default; do
                current_outputdir="$outputdir/$anomaly_scorer-$alert_triggerer"
                mkdir -p "$current_outputdir"
                
                for batching in 0 1 2 4 8 16 32 64 128; do
                    for pardeg in $(seq 1 $(($nproc / 5))); do
                        ./mo --duration="$duration" \
                             --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                             --batch="$batching,$batching,$batching,$batching" \
                             --chaining=false \
                             --rate="$rate" \
                             --execmode="$execmode" \
                             --anomalyscorer="$anomaly_scorer" \
                             --alerttriggerer="$alert_triggerer" \
                             --outputdir="$current_outputdir" \
                             >> "$current_outputdir/output-$($datecmd).txt"
                    done

                    for pardeg in $(seq 1 $(($nproc / 2))); do
                        ./mo --duration="$duration" \
                             --parallelism="$pardeg,$pardeg,$pardeg,$pardeg,$pardeg" \
                             --batch="$batching,$batching,$batching,$batching" \
                             --chaining=true \
                             --rate="$rate" \
                             --execmode="$execmode" \
                             --anomalyscorer="$anomaly_scorer" \
                             --alerttriggerer="$alert_triggerer" \
                             --outputdir="$current_outputdir" \
                             >> "$current_outputdir/output-$($datecmd).txt"
                    done
                done
            done
        done
    done
done

echo "Test completed on $(date)"

