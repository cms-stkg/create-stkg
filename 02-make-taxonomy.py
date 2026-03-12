#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Creates the STKG taxonomy from the STKG taxonomy file and schema

Call:
  python3 02-make-taxonomy.py --outdir yago-data/KG1

Input:
- <outdir>/01-stkg-final-schema.ttl
- input-data/stkg/stkg-taxonomy.ttl

Output:
- <outdir>/02-stkg-taxonomy.tsv

Algorithm:
1) Load the final schema
2) Load the taxonomy
3) Extract rdfs:subClassOf links
4) Remove invalid/self-loop/cyclic links
5) Write the resulting taxonomy
"""

import os
import argparse
from collections import defaultdict
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS

TAXONOMY_FILE = "input-data/stkg/stkg-taxonomy.ttl"

STKG = Namespace("http://example.org/stkg/")


def ensure_inputs(schema_file, taxonomy_file, output_folder):
    if not os.path.exists(schema_file):
        print(f"  Schema file {schema_file} not found\nfailed")
        exit(1)

    if not os.path.exists(taxonomy_file):
        print(f"  Taxonomy file {taxonomy_file} not found\nfailed")
        exit(1)

    os.makedirs(output_folder, exist_ok=True)


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
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="yago-data", help="output directory")
    args = ap.parse_args()

    output_folder = args.outdir
    schema_file = os.path.join(output_folder, "01-stkg-final-schema.ttl")
    output_file = os.path.join(output_folder, "02-stkg-taxonomy.tsv")

    print("Step 02: Creating STKG taxonomy...")

    ensure_inputs(schema_file, TAXONOMY_FILE, output_folder)

    print(f"  Loading schema from {schema_file} ...", end="", flush=True)
    schema_graph = load_graph(schema_file)
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

    print(f"  Writing taxonomy to {output_file} ...", end="", flush=True)
    write_taxonomy_tsv(cleaned_taxonomy, output_file)
    print("done")

    print("done")


if __name__ == "__main__":
    main()