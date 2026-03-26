#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exports final STKG TSV files into RDF serializations.

Call:
  python3 07-export.py --outdir yago-data/KG1

Input:
- <outdir>/01-stkg-final-schema.ttl
- <outdir>/05-stkg-final-taxonomy.tsv
- <outdir>/05-stkg-final-entities.tsv
- <outdir>/05-stkg-final-observations.tsv
- <outdir>/05-stkg-final-relations.tsv
- <outdir>/05-stkg-final-meta.tsv

Output:
- <outdir>/stkg-final.ttl
- <outdir>/stkg-final.nt
- <outdir>/stkg-tiny.ttl
"""

import os
import re
import argparse
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD


_TYPED_LITERAL_RE = re.compile(r'^"(.*)"\^\^<([^>]+)>$')


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


def parse_object(value: str):
    if value.startswith("http://") or value.startswith("https://"):
        return URIRef(value)

    m = _TYPED_LITERAL_RE.match(value)
    if m:
        lexical = m.group(1).replace('\\"', '"').replace("\\\\", "\\")
        datatype = m.group(2)
        return Literal(lexical, datatype=URIRef(datatype))

    if value.startswith('"') and value.endswith('"'):
        return Literal(value[1:-1])

    return URIRef(value)


def add_tsv_to_graph(g: Graph, path: str):
    for s, p, o in read_tsv(path):
        subj = URIRef(s)
        pred = URIRef(p) if p.startswith("http://") else URIRef(RDFS.subClassOf if p == "rdfs:subClassOf" else p)
        obj = parse_object(o)
        g.add((subj, pred, obj))


def build_tiny_graph(full_graph: Graph):
    """
    Tiny version:
    - keep schema
    - keep taxonomy
    - keep a small subset of entity/observation/relation facts
    """
    tiny = Graph()
    for prefix, ns in full_graph.namespaces():
        tiny.bind(prefix, ns)

    kept_subjects = set()
    count_resources = 0

    for s, p, o in full_graph:
        if p in {RDF.type, RDFS.domain, RDFS.range, RDFS.label, RDFS.subClassOf}:
            tiny.add((s, p, o))

    for s, p, o in full_graph:
        s_str = str(s)
        if "example.org/stkg/id/" in s_str or "example.org/stkg/platform/" in s_str or "example.org/stkg/obs/" in s_str:
            if s not in kept_subjects:
                if count_resources >= 200:
                    continue
                kept_subjects.add(s)
                count_resources += 1
            tiny.add((s, p, o))

    return tiny


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="yago-data", help="output directory")
    args = ap.parse_args()

    folder = args.outdir

    schema_file = os.path.join(folder, "01-stkg-final-schema.ttl")
    taxonomy_file = os.path.join(folder, "05-stkg-final-taxonomy.tsv")
    entities_file = os.path.join(folder, "05-stkg-final-entities.tsv")
    observations_file = os.path.join(folder, "05-stkg-final-observations.tsv")
    relations_file = os.path.join(folder, "05-stkg-final-relations.tsv")
    meta_file = os.path.join(folder, "05-stkg-final-meta.tsv")

    out_ttl = os.path.join(folder, "stkg-final.ttl")
    out_nt = os.path.join(folder, "stkg-final.nt")
    out_tiny = os.path.join(folder, "stkg-tiny.ttl")

    print("Step 07: Exporting STKG...")

    ensure_inputs([
        schema_file,
        taxonomy_file,
        entities_file,
        observations_file,
        relations_file,
        meta_file,
    ])

    g = Graph()

    print(f"  Loading schema from {schema_file} ...", end="", flush=True)
    g.parse(schema_file, format="turtle")
    print("done")

    print("  Merging final TSV outputs ...", end="", flush=True)
    for path in [taxonomy_file, entities_file, observations_file, relations_file, meta_file]:
        add_tsv_to_graph(g, path)
    print("done")

    print(f"  Writing Turtle to {out_ttl} ...", end="", flush=True)
    g.serialize(destination=out_ttl, format="turtle")
    print("done")

    print(f"  Writing N-Triples to {out_nt} ...", end="", flush=True)
    g.serialize(destination=out_nt, format="nt")
    print("done")

    print(f"  Creating tiny graph {out_tiny} ...", end="", flush=True)
    tiny = build_tiny_graph(g)
    tiny.serialize(destination=out_tiny, format="turtle")
    print("done")

    print(f"  Info: Total triples exported: {len(g)}")
    print(f"  Info: Tiny triples exported: {len(tiny)}")


if __name__ == "__main__":
    main()