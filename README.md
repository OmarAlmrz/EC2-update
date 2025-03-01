sudo apt update
sudo apt install python3-venv

python3 -m venv myenv

pip install -r requirements.txt

source myenv/bin/activate

deactivate