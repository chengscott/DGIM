#!/bin/bash -xe
# mvn tidy:pom
# java-format -i src/*.java src/*/*

mvn clean package

input=$INPUT
output=$OUTPUT
[ -z "$input" ] && input="data/dgim"
[ -z "$output" ] && output="dgim"

#yarn jar target/DGIM-1.0.jar dgim.DGIM "$input" "$output"
