#!/bin/sh

cd $(dirname $0)
./sa-sentiment-analysis/sa-test.sh
./mo-machine-outlier/mo-test.sh
./tt-trending-topics/tt-test.sh
./rl-reinforcement-learner/rl-test.sh
./lp-log-processing/lp-test.sh
