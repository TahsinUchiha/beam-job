#!/bin/bash

# Define the required packages
REQUIRED_PACKAGES=("apache-beam[gcp]" "requests")

# Check if the required packages are installed
ALL_PACKAGES_INSTALLED=true
for PACKAGE in "${REQUIRED_PACKAGES[@]}"; do
    # Strip extras from the package name
    PACKAGE_NAME=${PACKAGE%%[*}
    if ! pip show "$PACKAGE_NAME" > /dev/null; then
        echo "$PACKAGE is not installed."
        ALL_PACKAGES_INSTALLED=false
    fi
done

# Define the virtual environment directory
VENV_DIR="venv"

# If not all required packages are installed, create and activate a virtual environment
if ! $ALL_PACKAGES_INSTALLED; then

    # Check if a virtual environment already exists
    if [ -d "$VENV_DIR" ]; then
        echo "Virtual environment already exists. Activating it..."
        source venv/bin/activate
        # Upgrade pip
        echo "Upgrading pip..."
        pip install --upgrade pip
    else
        echo "Creating virtual environment..."
        python3 -m venv "$VENV_DIR"

        # Activate the virtual environment
        echo "Activating virtual environment..."
        source "$VENV_DIR/bin/activate"

        # Upgrade pip
        echo "Upgrading pip..."
        pip install --upgrade pip
    fi
    # Perform a secondary check for the required packages inside the virtual environment
    echo "Checking if modules are in virtual environment..."
    for PACKAGE in "${REQUIRED_PACKAGES[@]}"; do
        # Strip extras from the package name
        PACKAGE_NAME=${PACKAGE%%[*}
        if ! pip show "$PACKAGE_NAME" > /dev/null; then
            echo "$PACKAGE is not installed in the virtual environment. Installing..."
            pip install "$PACKAGE"
        else
            echo "$PACKAGE is installed in the virtual environment."
        fi
    done
fi
