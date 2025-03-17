#!/bin/bash

# Define environment name
ENV_NAME="sma_env"

# Ensure Conda is installed
if ! command -v conda &> /dev/null; then
    echo "Error: Conda is not installed. Please install Conda and try again."
    exit 1
fi

# Create the Conda environment from the environment.yml file
echo "Creating Conda environment from environment.yml..."
conda env create -f environment.yml

# Check if the environment was created successfully
if ! conda info --envs | grep -q $ENV_NAME; then
    echo "Error: Failed to create Conda environment $ENV_NAME."
    exit 1
fi

# Activate the Conda environment
echo "Activating environment..."
source $(conda info --base)/etc/profile.d/conda.sh
conda activate $ENV_NAME

# Check if the activation was successful
if [ "$CONDA_DEFAULT_ENV" != "$ENV_NAME" ]; then
    echo "Error: Failed to activate Conda environment $ENV_NAME."
    exit 1
fi

# Install Jupyter and ipykernel
echo "Installing Jupyter and ipykernel..."
pip install jupyter ipykernel

# Add the Conda environment to Jupyter as a kernel
python -m ipykernel install --user --name=$ENV_NAME --display-name "Python ($ENV_NAME)"

echo "✅ Setup complete! Restart Jupyter and select kernel: Python ($ENV_NAME)"
