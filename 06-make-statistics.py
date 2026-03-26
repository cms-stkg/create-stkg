#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Produces statistics about STKG entities, observations, predicates, and taxonomy,
and extracts sample entities/observations.

Call:
  python3 06-make-statistics.py --outdir yago-data/KG1

Input:
- <outdir>/01-stkg-final-schema.ttl
- <outdir>/05-stkg-final-entities.tsv
- <outdir>/05-stkg-final-observations.tsv
- <outdir>/05-stkg-final-relations.tsv
- <outdir>/05-stkg-final-meta.tsv
- <outdir>/05-stkg-final-taxonomy.tsv

Output:
- <outdir>/06-statistics.txt
- <outdir>/06-taxonomy.html
- <outdir>/06-upper-taxonomy.html
- <outdir>/06-sample-entities.ttl

Algorithm:
- load schema
- load taxonomy
- run through final STKG facts
  - update statistics
  - collect class counts
  - sample resources
- print statistics and taxonomy trees
"""

import os
import glob
import random
import argparse
from collections import defaultdict

from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS


STKG = Namespace("http://example.org/stkg/")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

RDF_TYPE = str(RDF.type)
RDFS_SUBCLASS = "rdfs:subClassOf"

PLATFORM = str(STKG.Platform)
POSITION_OBS = str(STKG.PositionObservation)
SPATIAL_REL_OBS = str(STKG.SpatialRelationObservation)

OBSERVED_ENTITY = str(STKG.observedEntity)
SUBJECT_ENTITY = str(STKG.subjectEntity)
OBJECT_ENTITY = str(STKG.objectEntity)
RELATION_TYPE = str(STKG.relationType)
HAS_PREDICATE = str(STKG.hasPredicate)
TIME = str(STKG.time)
SOURCE_FILE = str(STKG.sourceFile)
SOURCE_ROW = str(STKG.sourceRow)

GEO_LAT = str(GEO.lat)
GEO_LONG = str(GEO.long)

PLATFORM_LOCAL = "Platform"
POSITION_OBS_LOCAL = "PositionObservation"
SPATIAL_REL_OBS_LOCAL = "SpatialRelationObservation"


def local_name(uri: str) -> str:
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    return uri.rstrip("/").rsplit("/", 1)[-1]


def ensure_inputs(required):
    for path in required:
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


def load_schema(schema_file):
    g = Graph()
    g.parse(schema_file, format="turtle")

    classes = set()
    properties = set()
    class_to_properties = defaultdict(set)
    property_to_range = defaultdict(set)
    property_to_domain = defaultdict(set)

    for s in g.subjects(RDF.type, RDFS.Class):
        classes.add(str(s))

    for p in g.subjects(RDF.type, RDF.Property):
        p_str = str(p)
        properties.add(p_str)
        for d in g.objects(p, RDFS.domain):
            class_to_properties[str(d)].add(p_str)
            property_to_domain[p_str].add(str(d))
        for r in g.objects(p, RDFS.range):
            property_to_range[p_str].add(str(r))

    return classes, properties, class_to_properties, property_to_domain, property_to_range


def load_taxonomy(taxonomy_file):
    taxonomy_up = defaultdict(set)
    taxonomy_down = defaultdict(set)

    for s, p, o in read_tsv(taxonomy_file):
        if p not in {RDFS_SUBCLASS, str(RDFS.subClassOf)}:
            continue
        taxonomy_up[s].add(o)
        taxonomy_down[o].add(s)

    return taxonomy_up, taxonomy_down


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


def group_by_subject(paths):
    grouped = defaultdict(list)
    for path in paths:
        for s, p, o in read_tsv(path):
            grouped[s].append((s, p, o))
    return grouped


def collect_subject_types(grouped_facts):
    subject_types = defaultdict(set)
    for s, facts in grouped_facts.items():
        for _, p, o in facts:
            if p == RDF_TYPE:
                subject_types[s].add(o)
    return subject_types


def get_super_classes(cls, taxonomy_up):
    return ancestors(cls, taxonomy_up)


def print_taxonomy_html(out_file, taxonomy_down, class_stats, root_classes):
    def _print_node(writer, cls):
        children = sorted(taxonomy_down.get(cls, []))
        if not children:
            writer.write(f"<li>{cls}: {class_stats.get(cls, 0)}</li>\n")
            return
        writer.write(
            f"<li><details style='margin-left: 2em'>"
            f"<summary style='margin-left: -2em'>{cls}: {class_stats.get(cls, 0)}</summary><ul>\n"
        )
        for child in children:
            _print_node(writer, child)
        writer.write("</ul></details></li>\n")

    with open(out_file, "w", encoding="utf-8") as writer:
        writer.write("""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>STKG Taxonomy</title>
<style>ul { list-style-type:none; }</style>
</head>
<body>
<h1>STKG Taxonomy</h1>
<ul>
""")
        for root in sorted(root_classes):
            _print_node(writer, root)
        writer.write("</ul></body></html>\n")


def print_upper_taxonomy_html(out_file, schema_classes, class_to_properties, property_to_domain, property_to_range, taxonomy_down):
    def _print_class(writer, cls, opened=False):
        props = sorted(class_to_properties.get(cls, []))
        children = sorted(taxonomy_down.get(cls, []))

        writer.write(
            f"<li><details style='margin-left: 2em'{' open' if opened else ''}>"
            f"<summary style='font-weight:bold; margin-left: -2em'>{cls}</summary>"
        )

        writer.write("<details style='margin-left: 2em'><summary style='margin-left: -2em'>Outgoing properties</summary><ul style='list-style-type:none'>\n")
        for p in props:
            ranges = ", ".join(sorted(property_to_range.get(p, []))) or "(unspecified)"
            writer.write(f"<li>- {p} &rarr; {ranges}</li>\n")
        writer.write("</ul></details>\n")

        writer.write("<details style='margin-left: 2em'><summary style='margin-left: -2em'>Subclasses</summary><ul style='list-style-type:none'>\n")
        for child in children:
            _print_class(writer, child)
        writer.write("</ul></details></details></li>\n")

    all_children = {c for children in taxonomy_down.values() for c in children}
    roots = sorted(c for c in schema_classes if c not in all_children)

    with open(out_file, "w", encoding="utf-8") as writer:
        writer.write("""<h1>STKG Schema</h1>
This is the top-level taxonomy of classes of STKG, together with their properties.
<ul style='list-style-type:none'>
""")
        for root in roots:
            _print_class(writer, root, opened=True)
        writer.write("</ul>\n")


def triple_to_ttl(s, p, o):
    s_ttl = f"<{s}>"
    p_ttl = f"<{p}>"

    if o.startswith('"') or o.startswith("_:"):
        o_ttl = o
    elif o.startswith("http://") or o.startswith("https://"):
        o_ttl = f"<{o}>"
    else:
        o_ttl = o

    return f"{s_ttl} {p_ttl} {o_ttl} .\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="yago-data", help="output directory")
    args = ap.parse_args()

    folder = args.outdir

    schema_file = os.path.join(folder, "01-stkg-final-schema.ttl")
    entities_file = os.path.join(folder, "05-stkg-final-entities.tsv")
    observations_file = os.path.join(folder, "05-stkg-final-observations.tsv")
    relations_file = os.path.join(folder, "05-stkg-final-relations.tsv")
    meta_file = os.path.join(folder, "05-stkg-final-meta.tsv")
    taxonomy_file = os.path.join(folder, "05-stkg-final-taxonomy.tsv")

    out_stats = os.path.join(folder, "06-statistics.txt")
    out_taxonomy_html = os.path.join(folder, "06-taxonomy.html")
    out_upper_taxonomy_html = os.path.join(folder, "06-upper-taxonomy.html")
    out_sample_ttl = os.path.join(folder, "06-sample-entities.ttl")

    print("Step 06: Collecting STKG statistics...")

    ensure_inputs([
        schema_file,
        entities_file,
        observations_file,
        relations_file,
        meta_file,
        taxonomy_file,
    ])

    print(f"  Loading schema from {schema_file} ...", end="", flush=True)
    schema_classes, schema_properties, class_to_properties, property_to_domain, property_to_range = load_schema(schema_file)
    print("done")

    print(f"  Loading taxonomy from {taxonomy_file} ...", end="", flush=True)
    taxonomy_up, taxonomy_down = load_taxonomy(taxonomy_file)
    print("done")

    predicate_stats = defaultdict(int)
    class_stats = defaultdict(int)

    for p in schema_properties:
        predicate_stats[p] = 0
    predicate_stats[RDF_TYPE] = 0

    for c in schema_classes:
        class_stats[c] = 0

    fact_files = [entities_file, observations_file, relations_file]
    grouped = group_by_subject(fact_files)
    subject_types = collect_subject_types(grouped)

    entity_count = 0
    platform_count = 0
    observation_count = 0
    position_obs_count = 0
    relation_obs_count = 0
    total_facts = 0
    total_classes_per_instance = 0

    samples = []
    random.seed(42)

    print("  Parsing final STKG facts ...", end="", flush=True)
    for subject, facts in grouped.items():
        entity_count += 1

        direct_types = subject_types.get(subject, set())
        inherited_types = set()
        for t in direct_types:
            inherited_types.update(get_super_classes(t, taxonomy_up))

        for c in inherited_types:
            class_stats[c] += 1
        total_classes_per_instance += len(inherited_types)

        inherited_local_names = {local_name(c) for c in inherited_types}

        if PLATFORM_LOCAL in inherited_local_names:
            platform_count += 1
        if POSITION_OBS_LOCAL in inherited_local_names:
            position_obs_count += 1
            observation_count += 1
        if SPATIAL_REL_OBS_LOCAL in inherited_local_names:
            relation_obs_count += 1
            observation_count += 1

        for _, p, _ in facts:
            predicate_stats[p] += 1
            total_facts += 1

        if len(samples) < 100:
            samples.append((subject, facts, inherited_types))
        else:
            if random.random() < 0.01:
                samples[random.randint(0, 99)] = (subject, facts, inherited_types)
    print("done")

    meta_facts = sum(1 for _ in read_tsv(meta_file))

    time_fact_count = predicate_stats.get(TIME, 0)
    relation_type_fact_count = predicate_stats.get(RELATION_TYPE, 0)
    has_predicate_fact_count = predicate_stats.get(HAS_PREDICATE, 0)
    geo_fact_count = predicate_stats.get(GEO_LAT, 0) + predicate_stats.get(GEO_LONG, 0)

    print("  Computing dump size ...", end="", flush=True)
    dump_size = 0
    for f in glob.glob(os.path.join(folder, "05-stkg-final-*.tsv")):
        dump_size += os.path.getsize(f)
    print("done")

    print(f"  Writing sample entities to {out_sample_ttl} ...", end="", flush=True)
    with open(out_sample_ttl, "w", encoding="utf-8") as writer:
        for subject, facts, inherited_types in samples:
            for s, p, o in facts:
                writer.write(triple_to_ttl(s, p, o))
            for c in sorted(inherited_types):
                writer.write(triple_to_ttl(subject, RDF_TYPE, c))
            writer.write("\n")
    print("done")

    avg_classes_per_instance = (total_classes_per_instance / entity_count) if entity_count else 0.0
    avg_facts_per_resource = (total_facts / entity_count) if entity_count else 0.0
    avg_facts_per_observation = (
        (predicate_stats.get(TIME, 0)
         + predicate_stats.get(OBSERVED_ENTITY, 0)
         + predicate_stats.get(SUBJECT_ENTITY, 0)
         + predicate_stats.get(OBJECT_ENTITY, 0)
         + predicate_stats.get(RELATION_TYPE, 0)
         + predicate_stats.get(HAS_PREDICATE, 0)
         + predicate_stats.get(GEO_LAT, 0)
         + predicate_stats.get(GEO_LONG, 0)) / observation_count
        if observation_count else 0.0
    )

    print(f"  Writing statistics to {out_stats} ...", end="", flush=True)
    with open(out_stats, "w", encoding="utf-8") as writer:
        writer.write("STKG statistics\n\n")
        writer.write(f"Dump size: {dump_size / 1024 / 1024:.4f} MB\n\n")
        writer.write(f"Total number of resources: {entity_count}\n")
        writer.write(f"  Platforms: {platform_count}\n")
        writer.write(f"  Observations: {observation_count}\n")
        writer.write(f"    PositionObservations: {position_obs_count}\n")
        writer.write(f"    SpatialRelationObservations: {relation_obs_count}\n\n")

        writer.write(f"Total number of classes: {len(set(list(schema_classes) + list(taxonomy_up.keys()) + list(taxonomy_down.keys())))}\n\n")
        writer.write(f"Total number of predicates: {len(predicate_stats)}\n")
        writer.write(f"Total number of facts: {total_facts}\n")
        writer.write(f"Total number of meta facts: {meta_facts}\n\n")

        writer.write(f"Avg number of classes per resource: {avg_classes_per_instance:.4f}\n")
        writer.write(f"Avg number of facts per resource: {avg_facts_per_resource:.4f}\n")
        writer.write(f"Avg number of facts per observation: {avg_facts_per_observation:.4f}\n\n")

        writer.write("STKG-specific predicate counts:\n")
        writer.write(f"  time: {time_fact_count}\n")
        writer.write(f"  relationType: {relation_type_fact_count}\n")
        writer.write(f"  hasPredicate: {has_predicate_fact_count}\n")
        writer.write(f"  geo facts (lat+long): {geo_fact_count}\n\n")

        writer.write("Predicate counts:\n")
        for pred, cnt in sorted(predicate_stats.items(), key=lambda x: (-x[1], x[0])):
            writer.write(f"  {pred}: {cnt}\n")

        writer.write("\nClass counts:\n")
        for cls, cnt in sorted(class_stats.items(), key=lambda x: (-x[1], x[0])):
            writer.write(f"  {cls}: {cnt}\n")
    print("done")

    all_children = {child for parent in taxonomy_down for child in taxonomy_down[parent]}
    all_nodes = set(taxonomy_up.keys()) | set(taxonomy_down.keys())
    root_classes = sorted(c for c in all_nodes if c not in all_children)

    print(f"  Writing taxonomy HTML to {out_taxonomy_html} ...", end="", flush=True)
    print_taxonomy_html(out_taxonomy_html, taxonomy_down, class_stats, root_classes)
    print("done")

    print(f"  Writing upper taxonomy HTML to {out_upper_taxonomy_html} ...", end="", flush=True)
    print_upper_taxonomy_html(
        out_upper_taxonomy_html,
        schema_classes,
        class_to_properties,
        property_to_domain,
        property_to_range,
        taxonomy_down,
    )
    print("done")

    print(f"  Info: Total resources: {entity_count}")
    print(f"  Info: Platforms: {platform_count}")
    print(f"  Info: Observations: {observation_count}")
    print(f"  Info: PositionObservations: {position_obs_count}")
    print(f"  Info: SpatialRelationObservations: {relation_obs_count}")
    print(f"  Info: Meta facts: {meta_facts}")
    print(f"  Info: Total facts: {total_facts}")


if __name__ == "__main__":
    main()