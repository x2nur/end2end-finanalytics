#!/bin/bash

mkdir -p data
curl -L -o ./data/transactions-datasets.zip\
  https://www.kaggle.com/api/v1/datasets/download/computingvictor/transactions-fraud-datasets
cd data 
unzip transactions-datasets.zip
