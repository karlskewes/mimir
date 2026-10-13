#!/usr/bin/env bash
cd "$(dirname "$0")"
./rgsim -out out -scn S1a,S1b,S2,S2b,S3,S4 -mode warm,cold -arr fixed,poisson -seeds 5 -par 8 > batch2.log 2> batch2.err
./rgsim -out out -scn S0 -mode warm,cold -arr fixedtight -seeds 5 -par 8 >> batch2.log 2>> batch2.err
echo BATCH_DONE >> batch2.log
