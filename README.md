# Setup

sudo apt update
sudo apt install python3-venv
python3 -m venv myenv

# Activate the virtual environment

source myenv/bin/activate
pip install -r requirements.txt

# Run the application

python update\_

# Run chroma instance (only if not running in docker container)

chroma run --path "/mnt/data/vectordb/" --port 8000
