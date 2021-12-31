#!/bin/sh

output_file=sa-test-output.txt
duration=120

if [ -f "$output_file" ]; then
    rm "$output_file"
fi

for chaining in false true; do
    for i in $(seq 1 28); do
	echo "Results with -t $duration -m $i -c $chaining" >> "$output_file"
	./sa -t $duration -m $i -c $chaining >> "$output_file"
	echo >> "$output_file"
    done
done
