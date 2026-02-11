## ----------------------------------------------------------------------
## Below is the contexts of codex cloud setup script
## ----------------------------------------------------------------------

# Create venv with latest pip
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip

# load our needed python packages
pip install -r pip-requirements.txt

# clone submodule repos
git submodule sync --recursive
git submodule update --init --recursive
