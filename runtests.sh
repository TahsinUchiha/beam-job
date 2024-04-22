#!/bin/bash

# Initialise environment, update pip..
if [ -f "./init.sh" ]; then
    ./init.sh
else
    echo "File ./init.sh does not exist."
    exit 1
fi
# Run tests and display output
python3 -m unittest "tests/transaction_processing_test.py"