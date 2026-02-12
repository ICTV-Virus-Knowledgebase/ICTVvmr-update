## ----------------------------------------------------------------------
## Below is the contexts of codex cloud setup script
## ----------------------------------------------------------------------

# Create venv with latest pip
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip

# load our needed python packages
pip install -r pip-requirements.txt

#
# clone submodule repos
#

# Codex env blocks SSH (port 22), so force HTTPS

# Try re-mapping
# (these didn't work - perhaps because codex stipes the quotes?)
#git config --global url."https://github.com/".insteadOf "git@github.com:"
#git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"
#git config --global url."https://github.com/".insteadOf "ssh://github.com/"
#git config --global url."https://github.com/".insteadOf "git://github.com/"
# Show what Git thinks the rewrites are (debug)
#git config --global --get-regexp '^url\..*\.insteadOf'

# Fall back to rewrite the specific URL in .gitmodules (local edit only)
git config -f .gitmodules submodule.test_data/export/ICTVdatabase.url \
  https://github.com/ICTV-Virus-Knowledgebase/ICTVdatabase.git

# do the actual submodule pull
git submodule sync --recursive
git submodule update --init --recursive
