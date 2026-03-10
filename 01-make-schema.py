#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Creates the STKG schema from stkg-schema.ttl

Call:
  python3 01-make-schema.py

Input:
- input-data/stkg/stkg-schema.ttl

Output:
- yago-data/01-stkg-final-schema.ttl

Algorithm:
1) Load the schema
2) Check basic consistency (RDF parse + required terms)
3) Write out the schema
"""

import os
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, XSD

OUTPUT_FOLDER = "yago-data/"
INPUT_FOLDER = "input-data/stkg"
INPUT_SCHEMA = os.path.join(INPUT_FOLDER, "stkg-schema.ttl")
OUTPUT_SCHEMA = os.path.join(OUTPUT_FOLDER, "01-stkg-final-schema.ttl")

STKG = Namespace("http://example.org/stkg/")
STKGREL = Namespace("http://example.org/stkg/relation/")
SCHEMA = Namespace("http://schema.org/")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")


def ensure_dirs():
    if not os.path.exists(INPUT_FOLDER):
        print(f"  Input folder {INPUT_FOLDER} not found\nfailed")
        exit(1)

    if not os.path.exists(INPUT_SCHEMA):
        print(f"  Input schema file {INPUT_SCHEMA} not found\nfailed")
        exit(1)

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def bind_prefixes(g: Graph):
    g.bind("stkg", STKG)
    g.bind("stkgrel", STKGREL)
    g.bind("schema", SCHEMA)
    g.bind("geo", GEO)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)


def validate_required_terms(g: Graph):
    required_classes = [
        STKG.Platform,
        STKG.PositionObservation,
        STKG.SpatialRelationObservation,
    ]

    required_properties = [
        STKG.observedEntity,
        STKG.time,
        STKG.subjectEntity,
        STKG.objectEntity,
        STKG.relationType,
        STKG.sourceFile,
        STKG.sourceRow,
    ]

    missing = []

    for cls in required_classes:
        if (cls, RDF.type, RDFS.Class) not in g:
            missing.append(f"Missing class: {cls}")

    for prop in required_properties:
        if (prop, RDF.type, RDF.Property) not in g:
            missing.append(f"Missing property: {prop}")

    if missing:
        print("  Schema validation failed:")
        for m in missing:
            print("   -", m)
        exit(1)


def main():
    print("Step 01: Creating STKG schema...")

    ensure_dirs()

    g = Graph()
    bind_prefixes(g)

    print(f"  Loading schema from {INPUT_SCHEMA} ...", end="", flush=True)
    g.parse(INPUT_SCHEMA, format="turtle")
    print("done")

    print("  Validating schema ...", end="", flush=True)
    validate_required_terms(g)
    print("done")

    print(f"  Writing schema to {OUTPUT_SCHEMA} ...", end="", flush=True)
    g.serialize(destination=OUTPUT_SCHEMA, format="turtle")
    print("done")

    print("done")


if __name__ == "__main__":
    main()