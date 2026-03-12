#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Replaces provisional STKG resource ids with canonical STKG ids

Call:
  python3 05-make-ids.py --outdir yago-data/KG1

Input:
- <outdir>/04-stkg-facts-checked.tsv
- <outdir>/04-stkg-ids.tsv
- <outdir>/04-stkg-bad-classes.tsv
- <outdir>/02-stkg-taxonomy.tsv

Output:
- <outdir>/05-stkg-final-observations.tsv
- <outdir>/05-stkg-final-relations.tsv
- <outdir>/05-stkg-final-meta.tsv
- <outdir>/05-stkg-final-taxonomy.tsv
- <outdir>/05-stkg-final-entities.tsv

Algorithm:
- load provisional id mappings
- remove bad classes from id map
- replace subject/object ids in checked facts
- split facts into entity / observation / relation / meta outputs
- rename taxonomy as well
"""

import os
import argparse

OUTPUT_DEFAULT = "yago-data"

STKG = "http://example.org/stkg/"
OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_SUBCLASS = "rdfs:subClassOf"

GEO_LAT = "http://www.w3.org/2003/01/geo/wgs84_pos#lat"
GEO_LONG = "http://www.w3.org/2003/01/geo/wgs84_pos#long"

POSITION_OBS = STKG + "PositionObservation"
SPATIAL_REL_OBS = STKG + "SpatialRelationObservation"
PLATFORM = STKG + "Platform"

OBSERVED_ENTITY = STKG + "observedEntity"
SUBJECT_ENTITY = STKG + "subjectEntity"
OBJECT_ENTITY = STKG + "objectEntity"
RELATION_TYPE = STKG + "relationType"
TIME = STKG + "time"
SOURCE_FILE = STKG + "sourceFile"
SOURCE_ROW = STKG + "sourceRow"


def ensure_inputs(paths):
    for path in paths:
        if not os.path.exists(path):
            raise FileNotFoundError(f"required input not found: {path}")


def read_tsv(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 3:
                continue
            yield parts[0], parts[1], parts[2]


def write_tsv(rows, path):
    with open(path, "w", encoding="utf-8") as f:
        for s, p, o in rows:
            f.write(f"{s}\t{p}\t{o}\n")


def is_uri(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def load_id_map(path):
    id_map = {}
    for s, p, o in read_tsv(path):
        if p != OWL_SAMEAS:
            continue
        id_map[s] = o
    return id_map


def load_bad_classes(path):
    bad = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if parts and parts[0]:
                bad.add(parts[0])
    return bad


def rename_entity(entity, id_map, bad_classes):
    if not is_uri(entity):
        return entity

    if entity in bad_classes:
        return None

    if entity in id_map:
        return id_map[entity]

    return entity


def classify_fact(s, p, o, subject_types):
    """
    Split facts into STKG-friendly output groups.
    """
    s_types = subject_types.get(s, set())

    if p in {SOURCE_FILE, SOURCE_ROW}:
        return "meta"

    if POSITION_OBS in s_types or SPATIAL_REL_OBS in s_types:
        if p in {OBSERVED_ENTITY, SUBJECT_ENTITY, OBJECT_ENTITY, RELATION_TYPE, TIME, GEO_LAT, GEO_LONG, RDF_TYPE}:
            if p == RDF_TYPE and o == SPATIAL_REL_OBS:
                return "relations"
            if SPATIAL_REL_OBS in s_types:
                return "relations"
            return "observations"

    if PLATFORM in s_types or (p == RDF_TYPE and o == PLATFORM):
        return "entities"

    if "/obs/" in s:
        if "/obs/rel/" in s:
            return "relations"
        return "observations"

    if "/platform/" in s:
        return "entities"

    return "meta"


def collect_subject_types(facts):
    subject_types = {}
    for s, p, o in facts:
        if p == RDF_TYPE:
            subject_types.setdefault(s, set()).add(o)
    return subject_types


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=OUTPUT_DEFAULT, help="output directory")
    args = ap.parse_args()

    output_folder = args.outdir

    in_facts = os.path.join(output_folder, "04-stkg-facts-checked.tsv")
    in_ids = os.path.join(output_folder, "04-stkg-ids.tsv")
    in_bad_classes = os.path.join(output_folder, "04-stkg-bad-classes.tsv")
    in_taxonomy = os.path.join(output_folder, "02-stkg-taxonomy.tsv")

    out_observations = os.path.join(output_folder, "05-stkg-final-observations.tsv")
    out_relations = os.path.join(output_folder, "05-stkg-final-relations.tsv")
    out_meta = os.path.join(output_folder, "05-stkg-final-meta.tsv")
    out_taxonomy = os.path.join(output_folder, "05-stkg-final-taxonomy.tsv")
    out_entities = os.path.join(output_folder, "05-stkg-final-entities.tsv")

    print("Step 05: Renaming STKG entities...")

    ensure_inputs([in_facts, in_ids, in_bad_classes, in_taxonomy])
    os.makedirs(output_folder, exist_ok=True)

    print(f"  Loading provisional ids from {in_ids} ...", end="", flush=True)
    id_map = load_id_map(in_ids)
    print("done")

    print(f"  Loading bad classes from {in_bad_classes} ...", end="", flush=True)
    bad_classes = load_bad_classes(in_bad_classes)
    print("done")

    for bad in list(bad_classes):
        id_map.pop(bad, None)

    print(f"  Loading checked facts from {in_facts} ...", end="", flush=True)
    raw_facts = list(read_tsv(in_facts))
    print("done")

    print("  Renaming checked facts ...", end="", flush=True)
    renamed_facts = []
    for s, p, o in raw_facts:
        new_s = rename_entity(s, id_map, bad_classes)
        if not new_s:
            continue

        if is_uri(o):
            new_o = rename_entity(o, id_map, bad_classes)
            if not new_o:
                continue
        else:
            new_o = o

        renamed_facts.append(((s, p, o), (new_s, p, new_o)))
    print("done")

    subject_types = collect_subject_types([raw for raw, _ in renamed_facts])

    observations_rows = []
    relations_rows = []
    meta_rows = []
    entity_rows = []

    print("  Splitting renamed facts ...", end="", flush=True)
    for (raw_s, p, raw_o), (new_s, _, new_o) in renamed_facts:
        bucket = classify_fact(raw_s, p, raw_o, subject_types)
        if bucket == "observations":
            observations_rows.append((new_s, p, new_o))
        elif bucket == "relations":
            relations_rows.append((new_s, p, new_o))
        elif bucket == "entities":
            entity_rows.append((new_s, p, new_o))
        else:
            meta_rows.append((new_s, p, new_o))
    print("done")

    print(f"  Writing observations to {out_observations} ...", end="", flush=True)
    write_tsv(observations_rows, out_observations)
    print("done")

    print(f"  Writing relations to {out_relations} ...", end="", flush=True)
    write_tsv(relations_rows, out_relations)
    print("done")

    print(f"  Writing entities to {out_entities} ...", end="", flush=True)
    write_tsv(entity_rows, out_entities)
    print("done")

    print(f"  Writing meta facts to {out_meta} ...", end="", flush=True)
    write_tsv(meta_rows, out_meta)
    print("done")

    print(f"  Renaming taxonomy from {in_taxonomy} ...", end="", flush=True)
    renamed_taxonomy = []
    for s, p, o in read_tsv(in_taxonomy):
        new_s = rename_entity(s, id_map, bad_classes)
        new_o = rename_entity(o, id_map, bad_classes)

        if not new_s or not new_o:
            continue

        renamed_taxonomy.append((new_s, p, new_o))
    print("done")

    print(f"  Writing final taxonomy to {out_taxonomy} ...", end="", flush=True)
    write_tsv(renamed_taxonomy, out_taxonomy)
    print("done")

    print(f"  Info: Renamed facts: {len(renamed_facts)}")
    print(f"  Info: Observation facts: {len(observations_rows)}")
    print(f"  Info: Relation facts: {len(relations_rows)}")
    print(f"  Info: Entity facts: {len(entity_rows)}")
    print(f"  Info: Meta facts: {len(meta_rows)}")
    print(f"  Info: Final taxonomy facts: {len(renamed_taxonomy)}")


if __name__ == "__main__":
    main()