#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exports final STKG TSV files into RDF serializations.

Input:
- yago-data/01-stkg-final-schema.ttl
- yago-data/05-stkg-final-taxonomy.tsv
- yago-data/05-stkg-final-entities.tsv
- yago-data/05-stkg-final-observations.tsv
- yago-data/05-stkg-final-relations.tsv
- yago-data/05-stkg-final-meta.tsv

Output:
- yago-data/stkg-final.ttl
- yago-data/stkg-final.nt
- yago-data/stkg-tiny.ttl
"""

import os
import re
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD

FOLDER = "yago-data/"

SCHEMA_FILE = os.path.join(FOLDER, "01-stkg-final-schema.ttl")
TAXONOMY_FILE = os.path.join(FOLDER, "05-stkg-final-taxonomy.tsv")
ENTITIES_FILE = os.path.join(FOLDER, "05-stkg-final-entities.tsv")
OBSERVATIONS_FILE = os.path.join(FOLDER, "05-stkg-final-observations.tsv")
RELATIONS_FILE = os.path.join(FOLDER, "05-stkg-final-relations.tsv")
META_FILE = os.path.join(FOLDER, "05-stkg-final-meta.tsv")

OUT_TTL = os.path.join(FOLDER, "stkg-final.ttl")
OUT_NT = os.path.join(FOLDER, "stkg-final.nt")
OUT_TINY = os.path.join(FOLDER, "stkg-tiny.ttl")


def ensure_inputs():
    for path in [
        SCHEMA_FILE,
        TAXONOMY_FILE,
        ENTITIES_FILE,
        OBSERVATIONS_FILE,
        RELATIONS_FILE,
        META_FILE,
    ]:
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


_TYPED_LITERAL_RE = re.compile(r'^"(.*)"\^\^<([^>]+)>$')


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
        # keep all schema/class/property definition triples
        if p in {RDF.type, RDFS.domain, RDFS.range, RDFS.label, RDFS.subClassOf}:
            tiny.add((s, p, o))

    # keep first ~200 non-schema resources
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
    print("Step 07: Exporting STKG...")

    ensure_inputs()

    g = Graph()

    print(f"  Loading schema from {SCHEMA_FILE} ...", end="", flush=True)
    g.parse(SCHEMA_FILE, format="turtle")
    print("done")

    print("  Merging final TSV outputs ...", end="", flush=True)
    for path in [TAXONOMY_FILE, ENTITIES_FILE, OBSERVATIONS_FILE, RELATIONS_FILE, META_FILE]:
        add_tsv_to_graph(g, path)
    print("done")

    print(f"  Writing Turtle to {OUT_TTL} ...", end="", flush=True)
    g.serialize(destination=OUT_TTL, format="turtle")
    print("done")

    print(f"  Writing N-Triples to {OUT_NT} ...", end="", flush=True)
    g.serialize(destination=OUT_NT, format="nt")
    print("done")

    print(f"  Creating tiny graph {OUT_TINY} ...", end="", flush=True)
    tiny = build_tiny_graph(g)
    tiny.serialize(destination=OUT_TINY, format="turtle")
    print("done")

    print(f"  Info: Total triples exported: {len(g)}")
    print(f"  Info: Tiny triples exported: {len(tiny)}")


if __name__ == "__main__":
    main()