#!/bin/bash

COMP="javac"
JUNIT="./lib/junit-4.12.jar"
HAMCREST="./lib/hamcrest-core-1.3.jar"
AWS="./lib/aws-java-sdk-1.9.13.jar"
JACKSONANN="./lib/jackson-annotations-2.4.0.jar"
JACKSONDB="./lib/jackson-databind-2.4.4.jar"
APACHELOG="./lib/commons-logging-1.2.jar"
APACHEHTTPCLIENT="./lib/httpclient-4.3.6.jar"
APACHEHTTPCORE="./lib/httpcore-4.3.6.jar"
CLASSPATH=".:$HAMCREST:$JUNIT:$AWS:$JACKSONANN:$JACKSONDB:$APACHELOG:$APACHEHTTPCORE:$APACHEHTTPCLIENT"
OUTDIR="./bin"

EXTRA="-Xlint:unchecked"

FILES=`find ./src/ -name '*.java'`

mkdir "$OUTDIR" 2> /dev/null

cmd=`echo ""$COMP" "$EXTRA" -classpath "$CLASSPATH" -d "$OUTDIR" "$FILES""`
echo "Compilation command: \"$cmd\" ";

echo "-------------------------------"

$cmd

if [ $? -eq 0 ]; then
        echo "Compilation succeeded!";
else
        echo "Compilation failed!";
fi

