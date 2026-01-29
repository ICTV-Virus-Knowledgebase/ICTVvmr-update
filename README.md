# ICTV VMR update

## Conda environment notes (Ubuntu)
On Ubuntu, environment solves can fail unless `conda-forge` is listed before `bioconda`.
If ordering alone is not sufficient, add channels exactly in the order shown below 
(from top to bottom) and set channel priority to strict:

```bash
conda config --add channels defaults
conda config --add channels conda-forge
conda config --add channels bioconda
conda config --set channel_priority strict
```
