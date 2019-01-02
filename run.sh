#!/bin/bash -xe
# mvn tidy:pom
# java-format -i src/*.java src/*/*

mvn clean package

input=$INPUT
output=$OUTPUT
iters=$ITERS
ws=$WINDOW
[ -z "$input" ] && input="data/dgim"
[ -z "$output" ] && output="dgim"
[ -z "$iters" ] && iters=10
[ -z "$ws" ] && ws=50

yarn jar target/DGIM-1.0.jar dgim.DGIM "$input" "$output" "$iters" "$ws"
