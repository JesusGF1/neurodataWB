
conda create -n allensdk python=3.9 -y
conda init
conda activate allensdk
pip install --no-input allensdk
pip install --no-input ipykernel
python -m ipykernel install --user --name=allensdk