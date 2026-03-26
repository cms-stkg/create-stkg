#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Type-checks STKG facts using the STKG schema and taxonomy

Call:
  python3 04-make-typecheck.py --outdir yago-data/KG1

Input:
- <outdir>/01-stkg-final-schema.ttl
- <outdir>/02-stkg-taxonomy.tsv
- <outdir>/03-stkg-facts.tsv

Output:
- <outdir>/04-stkg-facts-checked.tsv
- <outdir>/04-stkg-ids.tsv
- <outdir>/04-stkg-bad-classes.tsv

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
import csv
import argparse
from collections import defaultdict
from urllib.parse import quote

from rdflib import Graph
from rdflib.namespace import RDF, RDFS, XSD


STKG = "http://example.org/stkg/"
STKGREL = "http://example.org/stkg/relation/"
OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs"

GEO_LAT = "http://www.w3.org/2003/01/geo/wgs84_pos#lat"
GEO_LONG = "http://www.w3.org/2003/01/geo/wgs84_pos#long"

RDF_TYPE = str(RDF.type)
RDFS_SUBCLASS = str(RDFS.subClassOf)
RDFS_RESOURCE = str(RDFS.Resource)

XSD_DATETIME = str(XSD.dateTime)
XSD_DECIMAL = str(XSD.decimal)
XSD_INTEGER = str(XSD.integer)
XSD_STRING = str(XSD.string)
XSD_NS = "http://www.w3.org/2001/XMLSchema#"


def ensure_inputs(schema_file, taxonomy_file, facts_file):
    for path in [schema_file, taxonomy_file, facts_file]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"required input not found: {path}")


def read_tsv(path):
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        for parts in reader:
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


def subject_matches_domain(s, p, property_domain, instances, taxonomy_up):
    dom = property_domain.get(p)
    if not dom:
        return True
    if dom == RDFS_RESOURCE:
        return is_uri(s)
    return instance_of(s, dom, instances, taxonomy_up)


def object_matches_schema_range(kind, obj_val, obj_dt, rng, instances, taxonomy_up):
    if not rng:
        return False

    if rng.startswith(XSD_NS):
        return kind == "literal" and obj_dt == rng

    if rng == RDFS_RESOURCE:
        return kind == "uri"

    return kind == "uri" and instance_of(obj_val, rng, instances, taxonomy_up)


def validate_fact(s, p, o, property_domain, property_range, instances, taxonomy_up, schema_classes):
    """
    Returns True if the fact is valid under the STKG schema/taxonomy.
    """

    if not is_uri(s):
        return False

    kind, obj_val, obj_dt = parse_typed_literal(o)

    if p == RDF_TYPE:
        if kind != "uri":
            return False
        if obj_val in schema_classes:
            return True
        if obj_val in taxonomy_up:
            return True
        return False

    # schema domain 체크
    if not subject_matches_domain(s, p, property_domain, instances, taxonomy_up):
        return False

    # STKG 핵심 속성별 체크
    if p == STKG + "observedEntity":
        return kind == "uri" and instance_of(obj_val, STKG + "Platform", instances, taxonomy_up)

    if p == STKG + "subjectEntity":
        return kind == "uri" and instance_of(obj_val, STKG + "Platform", instances, taxonomy_up)

    if p == STKG + "objectEntity":
        return kind == "uri" and instance_of(obj_val, STKG + "Platform", instances, taxonomy_up)

    if p == STKG + "relationType":
        return kind == "uri" and obj_val.startswith(STKGREL)

    if p == STKG + "hasPredicate":
        return (
            kind == "uri"
            and obj_val.startswith(STKGREL)
            and instance_of(s, STKG + "PositionObservation", instances, taxonomy_up)
        )

    if p == STKG + "time":
        return kind == "literal" and obj_dt == XSD_DATETIME

    if p == GEO_LAT or p == GEO_LONG:
        return kind == "literal" and obj_dt == XSD_DECIMAL and safe_decimal(obj_val)

    if p == STKG + "sourceRow":
        return kind == "literal" and obj_dt == XSD_INTEGER and safe_int(obj_val)

    if p == STKG + "sourceFile":
        return kind == "literal" and (obj_dt is None or obj_dt == XSD_STRING)

    # 일반 schema range 체크
    rng = property_range.get(p)
    if rng:
        return object_matches_schema_range(kind, obj_val, obj_dt, rng, instances, taxonomy_up)

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
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="yago-data", help="output directory")
    args = ap.parse_args()

    output_folder = args.outdir
    schema_file = os.path.join(output_folder, "01-stkg-final-schema.ttl")
    taxonomy_file = os.path.join(output_folder, "02-stkg-taxonomy.tsv")
    facts_file = os.path.join(output_folder, "03-stkg-facts.tsv")

    out_facts = os.path.join(output_folder, "04-stkg-facts-checked.tsv")
    out_ids = os.path.join(output_folder, "04-stkg-ids.tsv")
    out_bad_classes = os.path.join(output_folder, "04-stkg-bad-classes.tsv")

    print("Step 04: Type-checking STKG facts...")

    ensure_inputs(schema_file, taxonomy_file, facts_file)

    print(f"  Loading schema from {schema_file} ...", end="", flush=True)
    schema_classes, property_domain, property_range = load_schema(schema_file)
    print("done")

    print(f"  Loading taxonomy from {taxonomy_file} ...", end="", flush=True)
    taxonomy_up, taxonomy_classes = load_taxonomy(taxonomy_file)
    print("done")

    print(f"  Loading instances from {facts_file} ...", end="", flush=True)
    instances = load_instances(facts_file)
    print("done")

    valid_rows = []
    id_rows = []
    seen_id_subjects = set()
    valid_fact_count = 0
    rejected_fact_count = 0

    valid_resources = set()

    print("  Type-checking facts ...", end="", flush=True)
    for s, p, o in read_tsv(facts_file):
        if validate_fact(s, p, o, property_domain, property_range, instances, taxonomy_up, schema_classes):
            valid_rows.append((s, p, o))
            valid_fact_count += 1
            valid_resources.add(s)

            kind, obj_val, _ = parse_typed_literal(o)
            if kind == "uri":
                valid_resources.add(obj_val)
        else:
            rejected_fact_count += 1
    print("done")

    for resource in sorted(valid_resources):
        if resource in seen_id_subjects:
            continue
        seen_id_subjects.add(resource)
        id_rows.append((resource, OWL_SAMEAS, provisional_id_uri(resource)))

    print(f"  Writing checked facts to {out_facts} ...", end="", flush=True)
    write_tsv(valid_rows, out_facts)
    print("done")

    print(f"  Writing provisional ids to {out_ids} ...", end="", flush=True)
    write_tsv(id_rows, out_ids)
    print("done")

    covered_classes = collect_nonempty_classes(instances, taxonomy_up)
    all_classes = set(schema_classes) | set(taxonomy_classes)
    bad_classes = sorted(c for c in all_classes if c.startswith(STKG) and c not in covered_classes)

    print(f"  Writing bad classes to {out_bad_classes} ...", end="", flush=True)
    write_tsv([(c, "", "") for c in bad_classes], out_bad_classes)
    print("done")

    print(f"  Info: Valid facts: {valid_fact_count}")
    print(f"  Info: Rejected facts: {rejected_fact_count}")
    print(f"  Info: Resources with provisional ids: {len(id_rows)}")
    print(f"  Info: Classes without instances: {len(bad_classes)}")


if __name__ == "__main__":
    main()