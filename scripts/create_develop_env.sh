#!/bin/bash
echo "Setting python3 venv"
python3 -m venv .venv
source .venv/bin/activate

echo "Download sJsonRpc"
git clone https://github.com/EduardoPagotto/sJsonRpc.git
cd sJsonRpc
echo "Install sJsonRpc"
pip3 install -r requirements.txt
pip3 install .
cd ..
echo "remove temp sJsonRpc"
rm -rf sJsonRpc

echo "Download SSF"
git clone https://github.com/EduardoPagotto/SSF.git
cd SSF
echo "Install SSF"
pip3 install -r requirements.txt
pip3 install .
cd ..
echo "remove temp SSF"
rm -rf SSF


echo "Deps Local"
pip3 install -r requirements.txt
echo "SUCESSO!!!!"
