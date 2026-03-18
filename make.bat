@echo off

set CSV=input-data/new_csv/KG2_tj_HX_lat_relX_relCH1_ver1.0_BTR.csv
set OUTDIR=yago-data5\KG1

echo Running STKG pipeline...

python 01-make-schema.py --outdir %OUTDIR%
python 02-make-taxonomy.py --outdir %OUTDIR%
python 03-make-facts.py --in %CSV% --out %OUTDIR%/03-stkg-facts.tsv
python 04-make-typecheck.py --outdir %OUTDIR%
python 05-make-ids.py --outdir %OUTDIR%
python 06-make-statistics.py --outdir %OUTDIR%
python 07-export.py --outdir %OUTDIR%

echo Pipeline finished.