#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Creates the STKG taxonomy from the STKG taxonomy file and schema

Call:
  python3 02-make-taxonomy.py

Input:
- yago-data/01-stkg-final-schema.ttl
- input-data/stkg/stkg-taxonomy.ttl

Output:
- yago-data/02-stkg-taxonomy.tsv

Algorithm:
1) Load the final schema
2) Load the taxonomy
3) Extract rdfs:subClassOf links
4) Remove invalid/self-loop/cyclic links
5) Write the resulting taxonomy
"""

import os
from collections import defaultdict
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS

OUTPUT_FOLDER = "yago-data/"
SCHEMA_FILE = os.path.join(OUTPUT_FOLDER, "01-stkg-final-schema.ttl")
TAXONOMY_FILE = "input-data/stkg/stkg-taxonomy.ttl"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "02-stkg-taxonomy.tsv")

STKG = Namespace("http://example.org/stkg/")


def ensure_inputs():
    if not os.path.exists(SCHEMA_FILE):
        print(f"  Schema file {SCHEMA_FILE} not found\nfailed")
        exit(1)

    if not os.path.exists(TAXONOMY_FILE):
        print(f"  Taxonomy file {TAXONOMY_FILE} not found\nfailed")
        exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def load_graph(path: str) -> Graph:
    g = Graph()
    g.parse(path, format="turtle")
    return g


def get_schema_classes(schema_graph: Graph):
    classes = set()
    for s in schema_graph.subjects(RDF.type, RDFS.Class):
        classes.add(s)
    return classes


def extract_taxonomy_links(taxonomy_graph: Graph):
    taxonomy_up = defaultdict(set)
    for subcls, _, supercls in taxonomy_graph.triples((None, RDFS.subClassOf, None)):
        taxonomy_up[subcls].add(supercls)
    return taxonomy_up


def ancestors(node, taxonomy_up, visited=None):
    if visited is None:
        visited = set()
    if node in visited:
        return set()
    visited.add(node)

    result = {node}
    for parent in taxonomy_up.get(node, set()):
        result.update(ancestors(parent, taxonomy_up, visited.copy()))
    return result


def is_valid_stkg_class(uri):
    return str(uri).startswith(str(STKG))


def clean_taxonomy_links(taxonomy_up, schema_classes):
    cleaned = defaultdict(set)

    invalid_count = 0
    self_loop_count = 0
    cycle_count = 0

    for subcls in taxonomy_up:
        for supercls in taxonomy_up[subcls]:
            # only allow STKG namespace classes
            if not is_valid_stkg_class(subcls) or not is_valid_stkg_class(supercls):
                invalid_count += 1
                continue

            # remove self-loop
            if subcls == supercls:
                self_loop_count += 1
                continue

            # tentative add then cycle check
            cleaned[subcls].add(supercls)
            if subcls in ancestors(supercls, cleaned):
                cleaned[subcls].remove(supercls)
                cycle_count += 1

    return cleaned, invalid_count, self_loop_count, cycle_count


def write_taxonomy_tsv(taxonomy_up, out_path):
    with open(out_path, "w", encoding="utf-8") as f:
        for subcls in sorted(taxonomy_up.keys(), key=lambda x: str(x)):
            for supercls in sorted(taxonomy_up[subcls], key=lambda x: str(x)):
                f.write(f"{subcls}\trdfs:subClassOf\t{supercls}\n")


def main():
    print("Step 02: Creating STKG taxonomy...")

    ensure_inputs()

    print(f"  Loading schema from {SCHEMA_FILE} ...", end="", flush=True)
    schema_graph = load_graph(SCHEMA_FILE)
    schema_classes = get_schema_classes(schema_graph)
    print("done")

    print(f"  Loading taxonomy from {TAXONOMY_FILE} ...", end="", flush=True)
    taxonomy_graph = load_graph(TAXONOMY_FILE)
    taxonomy_up = extract_taxonomy_links(taxonomy_graph)
    print("done")

    print(f"  Info: Total taxonomy classes: {len(taxonomy_up)}")
    print(f"  Info: Total taxonomy links: {sum(len(v) for v in taxonomy_up.values())}")

    print("  Cleaning taxonomy links ...", end="", flush=True)
    cleaned_taxonomy, invalid_count, self_loop_count, cycle_count = clean_taxonomy_links(
        taxonomy_up, schema_classes
    )
    print("done")

    print(f"  Info: Invalid links removed: {invalid_count}")
    print(f"  Info: Self-loops removed: {self_loop_count}")
    print(f"  Info: Cycles removed: {cycle_count}")

    print(f"  Writing taxonomy to {OUTPUT_FILE} ...", end="", flush=True)
    write_taxonomy_tsv(cleaned_taxonomy, OUTPUT_FILE)
    print("done")

    print("done")


if __name__ == "__main__":
    main()