#!/bin/sh
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p $HOME/miniconda
$HOME/miniconda/bin/conda create -y -n demo python=3.8
$HOME/miniconda/bin/conda init bash
exec bash
conda activate demo
conda install -y -c conda-forge python-avro
conda install -y faker
rm -rf miniconda.sh
sudo mkdir /data
sudo chmod 0770 /data
sudo chgrp -R users /data
git clone https://github.com/ykzj/data-generator dg
cd dg
python datagen.py