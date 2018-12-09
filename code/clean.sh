#!/bin/sh

rm -f log/*
rm -f state/*
rm -f summary.perf
rm -f input.rs
find . -type f -path "./src/*/*" -name "*.class" -delete
find . -type f -path "./bin/*/*" -name "*.class" -delete
