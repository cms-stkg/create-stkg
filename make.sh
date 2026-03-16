#!/usr/bin/env bash
set -e

CSV="input-data/new_csv/KG3_HX_lat_relX_relCH2_ver2.0.csv"
OUTDIR="yago-data/KG3"

echo "Running STKG pipeline..."

python3 01-make-schema.py --outdir "$OUTDIR"
python3 02-make-taxonomy.py --outdir "$OUTDIR"
python3 03-make-facts.py --in "$CSV" --out "$OUTDIR/03-stkg-facts.tsv"
python3 04-make-typecheck.py --outdir "$OUTDIR"
python3 05-make-ids.py --outdir "$OUTDIR"
python3 06-make-statistics.py --outdir "$OUTDIR"
python3 07-export.py --outdir "$OUTDIR"

echo "Pipeline finished."