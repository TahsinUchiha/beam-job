#!/bin/bash

# Initialise environment, update pip..
if [ -f "./init.sh" ]; then
    ./init.sh
else
    echo "File ./init.sh does not exist."
    exit 1
fi

# Run the Apache Beam pipeline
echo "Running the Apache Beam pipeline..."
python3 "beam_pipeline_composite.py"

# Ask the user if they want to check the contents of the file 
echo "Listing the contents of the output directory..."
ls output
echo "Do you want to unzip the file in the output directory? (Y/n)"
read RESPONSE
if [[ $RESPONSE == "Y" || $RESPONSE == "y" ]]; then
    echo "Decompressing gzip file for viewing, while keeping original..."
    for file in output/*.gz
    do
      gunzip -k "$file"
    done
    echo "Files decompressed successfully."
fi
cd output

# Output the file 
echo "Job completed successfully."
echo "Outputting the first 10 records of JSONL file.."
if ls *.jsonl 1> /dev/null 2>&1; then
    head -n 10 $(ls *.jsonl | head -n 1)
else
    echo "No .jsonl files found in the output directory."
fi