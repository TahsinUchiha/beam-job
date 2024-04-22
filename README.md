# Apache Beam Job

This project contains Apache Beam jobs defined in `beam_pipeline.py` and `beam_pipeline_composite.py`. 
These jobs can be executed from executed from `runbeam.sh` script.
Tests can also be executed by running `runtests.sh`.

## Documentation

Detailed documentation on the steps and procedures taken during the development of the Apache Beam jobs, as well as instructions on 
how to reproduce the steps, can be found in the [Documentation.md](docs/Documentation.md) file or more 
intuitively [Documentation.ipynb](docs/Documentation.ipynb).

## Getting Started

To get started with this job, simply clone this repository,  
then execute on a unix terminal `./runbeam.sh` or `./runtests.sh` this will either create a virtual environment and install any dependencies 
or directly run the beam if all modules are present.  
For Windows, you can trying using "wsl" and simply run the above scripts, else make sure you have python3 and have the necessary modules, can install via pip:  
```
pip install apache-beam[gcp]
pip install requests 
```
Then run:  
`python3 beam_pipeline_composite.py`  
To run tests:  
`python3 "tests/transaction_processing_test.py`
