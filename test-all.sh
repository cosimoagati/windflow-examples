#!/bin/sh

cd sa-sentiment-analysis
./sa-test.sh
cd ..

cd mo-machine-outlier
./mo-test.sh
cd ..

cd tt-trending-topics
./tt-test.sh
cd ..

cd rl-reinforcement-learner
./rl-test.sh
cd ..

cd lp-log-processing
./lp-test.sh
cd ..
