@echo off

set CSV=input-data\ver2.2\KG2_HX_lat_relX_relCH1_ver2.2_e2.csv
set OUTDIR=yago-data6\KG3

echo Running STKG pipeline...

python 01-make-schema.py --outdir %OUTDIR%
python 02-make-taxonomy.py --outdir %OUTDIR%
python 03-make-facts.py --in %CSV% --out %OUTDIR%/03-stkg-facts.tsv
python 04-make-typecheck.py --outdir %OUTDIR%
python 05-make-ids.py --outdir %OUTDIR%
python 06-make-statistics.py --outdir %OUTDIR%
python 07-export.py --outdir %OUTDIR%

echo Pipeline finished.