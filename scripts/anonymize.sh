#!/bin/sh

# Expecting llm_retrieval.zip to be in the same directory as this script

set -ex

git clone --recursive git@github.com:forecastingresearch/llm-benchmark.git

cd llm-benchmark
rm -rf .git .github .gitignore .gitmodules;

cd utils
rm -rf .git .github .gitignore .gitmodules;

cd ..
find . -type f -exec sed -i '' 's/ Forecasting Research Institute//g' {} +

cd ..
mv llm-benchmark forecastBench

unzip llm_retrieval.zip
rm -rf __MACOSX
mv llm_retrieval forecastBench/llm_retrieval
tar -cvJf forecastBench.tar.xz forecastBench/
