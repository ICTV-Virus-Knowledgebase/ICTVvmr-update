# ChatGPT Codex Environment Setup

#----------------------------------------------------------------------
# In 'environment setup script', which is not a file, but a codex environment setting
#----------------------------------------------------------------------

# Create venv with latest pip
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip

# load our needed python packages
pip install -r requirements.txt

# clone submodule repos
git submodule sync --recursive
git submodule update --init --recursive


#----------------------------------------------------------------------
# In [../../AGENTS.md](../../AGENTS.md) add
#----------------------------------------------------------------------

Details on:
  - Repository layout
  - Regression tests
  - Independent programs in the repo, etc


