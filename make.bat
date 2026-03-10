@echo off

set CSV=input-data/new_csv/KG1_HO_lat_relX_ver2.0.csv

echo Running STKG pipeline...

python 01-make-schema.py
python 02-make-taxonomy.py
python 03-make-facts.py --in %CSV% --out yago-data/03-stkg-facts.tsv
python 04-make-typecheck.py
python 05-make-ids.py
python 06-make-statistics.py
python 07-export.py

echo Pipeline finished.