#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Type-checks STKG facts using the STKG schema and taxonomy

Call:
  python3 04-make-typecheck.py

Input:
- yago-data/01-stkg-final-schema.ttl
- yago-data/02-stkg-taxonomy.tsv
- yago-data/03-stkg-facts.tsv

Output:
- yago-data/04-stkg-facts-checked.tsv
- yago-data/04-stkg-ids.tsv
- yago-data/04-stkg-bad-classes.tsv

Algorithm:
1) Load schema classes and property constraints
2) Load taxonomy
3) Load rdf:type instances from facts
4) Run through all facts and type-check them
5) Write valid facts only
6) Write provisional STKG ids for valid resources
7) Write classes that do not have instances
"""

import os
import re
from collections import defaultdict
from urllib.parse import quote

from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, XSD


OUTPUT_FOLDER = "yago-data/"
SCHEMA_FILE = os.path.join(OUTPUT_FOLDER, "01-stkg-final-schema.ttl")
TAXONOMY_FILE = os.path.join(OUTPUT_FOLDER, "02-stkg-taxonomy.tsv")
FACTS_FILE = os.path.join(OUTPUT_FOLDER, "03-stkg-facts.tsv")

OUT_FACTS = os.path.join(OUTPUT_FOLDER, "04-stkg-facts-checked.tsv")
OUT_IDS = os.path.join(OUTPUT_FOLDER, "04-stkg-ids.tsv")
OUT_BAD_CLASSES = os.path.join(OUTPUT_FOLDER, "04-stkg-bad-classes.tsv")


STKG = "http://example.org/stkg/"
STKGREL = "http://example.org/stkg/relation/"
OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs"

GEO_LAT = "http://www.w3.org/2003/01/geo/wgs84_pos#lat"
GEO_LONG = "http://www.w3.org/2003/01/geo/wgs84_pos#long"

RDF_TYPE = str(RDF.type)
RDFS_SUBCLASS = str(RDFS.subClassOf)
XSD_DATETIME = str(XSD.dateTime)
XSD_DECIMAL = str(XSD.decimal)
XSD_INTEGER = str(XSD.integer)
XSD_STRING = str(XSD.string)


def ensure_inputs():
    for path in [SCHEMA_FILE, TAXONOMY_FILE, FACTS_FILE]:
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
        for row in rows:
            f.write("\t".join(row) + "\n")


def is_uri(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


_TYPED_LITERAL_RE = re.compile(r'^"(.*)"\^\^<([^>]+)>$')


def parse_typed_literal(value: str):
    """
    Returns:
      ("literal", lexical_value, datatype_uri)
      or ("uri", value, None)
      or ("other", value, None)
    """
    if is_uri(value):
        return ("uri", value, None)

    m = _TYPED_LITERAL_RE.match(value)
    if m:
        lexical = m.group(1)
        datatype = m.group(2)
        lexical = lexical.replace('\\"', '"').replace("\\\\", "\\")
        return ("literal", lexical, datatype)

    return ("other", value, None)


def safe_decimal(s: str) -> bool:
    try:
        float(s)
        return True
    except Exception:
        return False


def safe_int(s: str) -> bool:
    try:
        int(s)
        return True
    except Exception:
        return False


def load_schema(schema_file):
    g = Graph()
    g.parse(schema_file, format="turtle")

    classes = set()
    property_domain = {}
    property_range = {}

    for s in g.subjects(RDF.type, RDFS.Class):
        classes.add(str(s))

    for p in g.subjects(RDF.type, RDF.Property):
        p_str = str(p)
        for d in g.objects(p, RDFS.domain):
            property_domain[p_str] = str(d)
        for r in g.objects(p, RDFS.range):
            property_range[p_str] = str(r)

    return classes, property_domain, property_range


def load_taxonomy(taxonomy_file):
    taxonomy_up = defaultdict(set)
    all_taxonomy_classes = set()

    for s, p, o in read_tsv(taxonomy_file):
        if p != "rdfs:subClassOf" and p != RDFS_SUBCLASS:
            continue
        taxonomy_up[s].add(o)
        all_taxonomy_classes.add(s)
        all_taxonomy_classes.add(o)

    return taxonomy_up, all_taxonomy_classes


def ancestors(c, taxonomy_up, visited=None):
    if visited is None:
        visited = set()
    if c in visited:
        return set()
    visited.add(c)

    result = {c}
    for parent in taxonomy_up.get(c, set()):
        result.update(ancestors(parent, taxonomy_up, visited.copy()))
    return result


def is_subclass_of(c1, c2, taxonomy_up):
    if c1 == c2:
        return True
    for parent in taxonomy_up.get(c1, set()):
        if is_subclass_of(parent, c2, taxonomy_up):
            return True
    return False


def load_instances(facts_file):
    instances = defaultdict(set)
    for s, p, o in read_tsv(facts_file):
        if p == RDF_TYPE and is_uri(o):
            instances[s].add(o)
    return instances


def instance_of(resource, target_class, instances, taxonomy_up):
    for c in instances.get(resource, set()):
        if is_subclass_of(c, target_class, taxonomy_up):
            return True
    return False


def local_name(uri: str) -> str:
    if "/" in uri:
        return uri.rstrip("/").split("/")[-1]
    if "#" in uri:
        return uri.split("#")[-1]
    return uri


def normalize_id_token(text: str) -> str:
    text = text.strip()
    text = re.sub(r"[^A-Za-z0-9_\-]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        text = "resource"
    return text


def provisional_id_uri(resource_uri: str) -> str:
    """
    04단계에서는 최종 canonical id가 아니라
    05단계에서 사용할 임시/stable id를 부여.
    """
    if resource_uri.startswith(STKG):
        suffix = resource_uri[len(STKG):]
    else:
        suffix = local_name(resource_uri)

    token = normalize_id_token(suffix.replace("/", "_"))
    return STKG + "id/" + quote(token)


def validate_fact(s, p, o, property_domain, property_range, instances, taxonomy_up, schema_classes):
    """
    Returns True if the fact is valid under the STKG schema/taxonomy.
    """

    # subject should be a URI
    if not is_uri(s):
        return False

    kind, obj_val, obj_dt = parse_typed_literal(o)

    # rdf:type validation
    if p == RDF_TYPE:
        if kind != "uri":
            return False
        if obj_val in schema_classes:
            return True
        if obj_val in taxonomy_up:
            return True
        return False

    # observedEntity -> range Platform
    if p == STKG + "observedEntity":
        return kind == "uri" and instance_of(obj_val, STKG + "Platform", instances, taxonomy_up)

    # subjectEntity / objectEntity -> range Platform
    if p == STKG + "subjectEntity":
        return kind == "uri" and instance_of(obj_val, STKG + "Platform", instances, taxonomy_up)

    if p == STKG + "objectEntity":
        return kind == "uri" and instance_of(obj_val, STKG + "Platform", instances, taxonomy_up)

    # relationType -> resource URI
    if p == STKG + "relationType":
        return kind == "uri"

    # time -> xsd:dateTime
    if p == STKG + "time":
        return kind == "literal" and obj_dt == XSD_DATETIME

    # geo lat/long -> decimal
    if p == GEO_LAT or p == GEO_LONG:
        return kind == "literal" and obj_dt == XSD_DECIMAL and safe_decimal(obj_val)

    # sourceRow -> integer
    if p == STKG + "sourceRow":
        return kind == "literal" and obj_dt == XSD_INTEGER and safe_int(obj_val)

    # sourceFile -> typed string preferred, but any literal 허용
    if p == STKG + "sourceFile":
        return kind == "literal"

    # For other schema properties, try generic range validation
    rng = property_range.get(p)
    if rng:
        if rng.startswith("http://www.w3.org/2001/XMLSchema#"):
            return kind == "literal" and obj_dt == rng
        else:
            return kind == "uri" and instance_of(obj_val, rng, instances, taxonomy_up)

    # Unknown property -> reject
    return False


def collect_nonempty_classes(instances, taxonomy_up):
    """
    Returns classes that have at least one instance, directly or via subclass chain.
    """
    covered = set()
    for _, classes in instances.items():
        for c in classes:
            covered.update(ancestors(c, taxonomy_up))
    return covered


def main():
    print("Step 04: Type-checking STKG facts...")

    ensure_inputs()

    print(f"  Loading schema from {SCHEMA_FILE} ...", end="", flush=True)
    schema_classes, property_domain, property_range = load_schema(SCHEMA_FILE)
    print("done")

    print(f"  Loading taxonomy from {TAXONOMY_FILE} ...", end="", flush=True)
    taxonomy_up, taxonomy_classes = load_taxonomy(TAXONOMY_FILE)
    print("done")

    print(f"  Loading instances from {FACTS_FILE} ...", end="", flush=True)
    instances = load_instances(FACTS_FILE)
    print("done")

    valid_rows = []
    id_rows = []
    seen_id_subjects = set()
    valid_fact_count = 0
    rejected_fact_count = 0

    valid_subjects = set()
    valid_resources = set()

    print("  Type-checking facts ...", end="", flush=True)
    for s, p, o in read_tsv(FACTS_FILE):
        if validate_fact(s, p, o, property_domain, property_range, instances, taxonomy_up, schema_classes):
            valid_rows.append((s, p, o))
            valid_fact_count += 1
            valid_subjects.add(s)
            valid_resources.add(s)

            kind, obj_val, _ = parse_typed_literal(o)
            if kind == "uri":
                valid_resources.add(obj_val)
        else:
            rejected_fact_count += 1
    print("done")

    # provisional ids for valid resources only
    for resource in sorted(valid_resources):
        if resource in seen_id_subjects:
            continue
        seen_id_subjects.add(resource)
        id_rows.append((resource, OWL_SAMEAS, provisional_id_uri(resource)))

    print(f"  Writing checked facts to {OUT_FACTS} ...", end="", flush=True)
    write_tsv(valid_rows, OUT_FACTS)
    print("done")

    print(f"  Writing provisional ids to {OUT_IDS} ...", end="", flush=True)
    write_tsv(id_rows, OUT_IDS)
    print("done")

    # classes with no instances
    covered_classes = collect_nonempty_classes(instances, taxonomy_up)
    all_classes = set(schema_classes) | set(taxonomy_classes)
    bad_classes = sorted(c for c in all_classes if c.startswith(STKG) and c not in covered_classes)

    print(f"  Writing bad classes to {OUT_BAD_CLASSES} ...", end="", flush=True)
    write_tsv([(c, "", "") for c in bad_classes], OUT_BAD_CLASSES)
    print("done")

    print(f"  Info: Valid facts: {valid_fact_count}")
    print(f"  Info: Rejected facts: {rejected_fact_count}")
    print(f"  Info: Resources with provisional ids: {len(id_rows)}")
    print(f"  Info: Classes without instances: {len(bad_classes)}")


if __name__ == "__main__":
    main()