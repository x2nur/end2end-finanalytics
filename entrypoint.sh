#!/usr/bin/env bash 

mkdir -p $HOME/workspace 
# SPARK_HOME=/usr/lib/spark
# SPARK_CONF_DIR=/etc/spark/conf
export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --ip=0.0.0.0 --ServerApp.root_dir=$HOME/workspace --IdentityProvider.token='sometokenhere' --ServerApp.allow_origin='*'"
umask 000
pyspark
