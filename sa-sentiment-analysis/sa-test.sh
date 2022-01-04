#!/bin/sh

output_file="sa-test-output-$(date --iso-8601).txt"
duration=120

if [ -f "$output_file" ]; then
    rm "$output_file"
fi

echo "Test started on $(date)" >> "$output_file"
echo >> "$output_file"

for chaining in false true; do
    for i in $(seq 1 28); do
	echo "Results with -t $duration -m $i -c $chaining" >> "$output_file"
	./sa -t $duration -m $i -c $chaining >> "$output_file"
	echo >> "$output_file"
    done
done

echo "Test completed on $(date)" >> "$output_file"
echo >> "$output_file"

