#!/usr/bin/env bash

set -e

CSV_INPUT="${1:-input-data/stkg/KG1.csv}"
FACTS_OUT="yago-data/03-stkg-facts.tsv"

echo "Running STKG pipeline with input: $CSV_INPUT"
date +"  Start time: %F %T"

python3 01-make-schema.py
python3 02-make-taxonomy.py
python3 03-make-facts.py --in "$CSV_INPUT" --out "$FACTS_OUT"
python3 04-make-typecheck.py
python3 05-make-ids.py
python3 06-make-statistics.py
python3 07-export.py

echo "STKG pipeline finished."
date +"  End time: %F %T"