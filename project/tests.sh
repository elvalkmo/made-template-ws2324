#!/bin/bash
pip install --upgrade pip
pip install -r /project/prequirements.txt

python3 project/pipeline.py
python3 project/test_output.py