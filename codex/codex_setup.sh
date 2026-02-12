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
# work around ssh block in codex env - map to https instead
git config --global url."https://github.com/".insteadOf "git@github.com:"
git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"
# do the actual submodule pull
git submodule sync --recursive
git submodule update --init --recursive
