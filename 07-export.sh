#!/usr/bin/env bash

set -e

cd yago-data

echo "Exporting STKG..."
date +"  Current time: %F %T"

cd ..
python3 07-export.py
cd yago-data

echo "Packing STKG files..."
rm -f stkg.zip
zip -q stkg.zip \
  stkg-final.ttl \
  stkg-final.nt \
  01-stkg-final-schema.ttl \
  05-stkg-final-taxonomy.tsv \
  05-stkg-final-entities.tsv \
  05-stkg-final-observations.tsv \
  05-stkg-final-relations.tsv \
  05-stkg-final-meta.tsv \
  06-statistics.txt \
  06-taxonomy.html \
  06-upper-taxonomy.html \
  06-sample-entities.ttl
echo "done"

echo "Packing tiny STKG..."
rm -f stkg-tiny.zip
zip -q stkg-tiny.zip stkg-tiny.ttl
echo "done"

date +"Current time: %F %T"