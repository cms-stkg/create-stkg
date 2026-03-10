#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03b-make-stkg.py
CSV(전장/trajectory 관측) -> STKG RDF 생성 (n-ary 필수, named graph 옵션)
- 입력: input-data/stkg/KG1_A.csv (사용자 포맷)
- 출력: yago-data/03b-stkg.{ttl,trig,nq}

실행 예)
python3 03b-make-stkg.py \
  --in input-data/stkg/KG1_A.csv \
  --out yago-data/03b-stkg \
  --graph_mode none \
  --emit ttl

python3 03b-make-stkg.py \
  --in input-data/stkg/KG1_A.csv \
  --out yago-data/03b-stkg \
  --graph_mode by_time \
  --emit trig

python3 03b-make-stkg.py \
  --in input-data/stkg/KG1_A.csv \
  --out yago-data/03b-stkg \
  --graph_mode by_file \
  --emit nq \
  --file_tag KG1_A
"""

import argparse
import csv
import os
import re
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict

from rdflib import Dataset, Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD


# ===== Namespaces =====
STKG = Namespace("http://example.org/stkg/")
STKGREL = Namespace("http://example.org/stkg/relation/")
SCHEMA = Namespace("http://schema.org/")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")


# ===== Time parsing (robust) =====
_TIME_PATTERNS = [
    "%Y-%m-%dT%H:%M:%S%z",   # 2026-02-05T22:36:32+0000
    "%Y-%m-%dT%H:%M:%S",     # 2026-02-05T22:36:32
    "%Y-%m-%d %H:%M:%S",     # 2026-02-05 22:36:32
    "%Y/%m/%d %H:%M:%S",     # 2026/02/05 22:36:32
    "%Y-%m-%d",              # 2026-02-05
]

def parse_time(timestr: str) -> datetime:
    """
    Return timezone-aware datetime in UTC if possible.
    If no timezone info, assume UTC.
    Accepts ISO8601 with 'Z' too.
    Also tolerates a leading '+' (e.g., +2026-02-05T22:36:32Z).
    """
    s = (timestr or "").strip()
    if not s:
        raise ValueError("Empty time string")

    # tolerate leading '+' in your CSV
    if s.startswith("+"):
        s = s[1:].strip()

    # Handle trailing Z
    if s.endswith("Z"):
        s2 = s[:-1]
        try:
            dt = datetime.fromisoformat(s2)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    # Try fromisoformat (handles offsets like +00:00)
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Try common patterns
    for p in _TIME_PATTERNS:
        try:
            dt = datetime.strptime(s, p)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue

    raise ValueError(f"Unrecognized time format: {timestr}")



def time_to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def time_to_key(dt: datetime) -> str:
    # URI-safe compact key
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ===== Relation normalization =====
def normalize_rel_fallback(s: str) -> str:
    """
    Fallback normalization: lower + non-alnum -> underscore.
    """
    s = (s or "").strip()
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s

def load_relation_map_tsv(path: Optional[str]) -> Dict[str, str]:
    """
    TSV format (tab-separated):
    raw_value <TAB> normalized_token

    Example:
    In Front Of    inFrontOf
    behind         behind
    """
    if not path:
        return {}
    if not os.path.exists(path):
        raise FileNotFoundError(f"relation map file not found: {path}")

    m: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            raw = parts[0].strip()
            norm = parts[1].strip()
            if raw and norm:
                m[raw.lower()] = norm
    return m

def normalize_relation(raw: str, rel_map: Dict[str, str]) -> str:
    """
    Use mapping table first; fallback to underscore token.
    """
    r = (raw or "").strip()
    if not r:
        return ""
    hit = rel_map.get(r.lower())
    if hit:
        return hit
    return normalize_rel_fallback(r)


# ===== Graph strategy =====
def graph_iri(mode: str, file_tag: str, time_key: str, platform_id: str) -> Optional[URIRef]:
    if mode == "none":
        return None
    if mode == "by_file":
        return STKG[f"graph/file/{file_tag}"]
    if mode == "by_time":
        return STKG[f"graph/time/{time_key}"]
    if mode == "by_platform":
        return STKG[f"graph/platform/{platform_id}"]
    raise ValueError(f"Unknown graph_mode: {mode}")


# ===== Helpers =====
def safe_decimal(s: str) -> Optional[Literal]:
    if s is None:
        return None
    ss = str(s).strip()
    if ss == "":
        return None
    try:
        # Keep lexical value clean
        v = float(ss)
        return Literal(f"{v:.10f}".rstrip("0").rstrip("."), datatype=XSD.decimal)
    except Exception:
        return None

def ensure_prefixes(g: Graph) -> None:
    g.bind("stkg", STKG)
    g.bind("stkgrel", STKGREL)
    g.bind("schema", SCHEMA)
    g.bind("geo", GEO)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("xsd", XSD)

def add_minimal_ontology(target_graph: Graph) -> None:
    """
    최소한의 클래스/프로퍼티 선언(실습용).
    나중에 stkg-schema.ttl / stkg-taxonomy.ttl를 별도로 로드/관리해도 됨.
    """
    # Classes
    target_graph.add((STKG.Platform, RDF.type, RDFS.Class))
    target_graph.add((STKG.PositionObservation, RDF.type, RDFS.Class))
    target_graph.add((STKG.SpatialRelationObservation, RDF.type, RDFS.Class))

    # Alignments
    target_graph.add((STKG.Platform, RDFS.subClassOf, SCHEMA.Thing))
    target_graph.add((STKG.PositionObservation, RDFS.subClassOf, SCHEMA.Event))
    target_graph.add((STKG.SpatialRelationObservation, RDFS.subClassOf, SCHEMA.Event))

    # Properties
    props = [
        STKG.observedEntity,
        STKG.time,
        STKG.subjectEntity,
        STKG.objectEntity,
        STKG.relationType,
        STKG.sourceFile,
        STKG.sourceRow,
    ]
    for p in props:
        target_graph.add((p, RDF.type, RDF.Property))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="input CSV")
    ap.add_argument("--out", dest="out", required=True, help="output path prefix (no extension)")
    ap.add_argument("--emit", choices=["ttl", "trig", "nq"], default="trig", help="output format")
    ap.add_argument("--graph_mode", choices=["none", "by_file", "by_time", "by_platform"], default="none")
    ap.add_argument("--file_tag", default="KG1_A", help="tag for by_file graph IRI")
    ap.add_argument("--relation_map", default="input-data/stkg/relation-map.tsv", help="TSV mapping for relation tokens")
    ap.add_argument("--load_schema", default="input-data/stkg/stkg-schema.ttl", help="optional schema ttl")
    ap.add_argument("--load_taxonomy", default="input-data/stkg/stkg-taxonomy.ttl", help="optional taxonomy ttl")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    rel_map = {}
    # relation map is recommended but optional
    if args.relation_map and os.path.exists(args.relation_map):
        rel_map = load_relation_map_tsv(args.relation_map)

    ds = Dataset()

    # Bind prefixes on default graph
    ensure_prefixes(ds.default_graph)

    # Add minimal ontology to default graph (so output alone is self-describing)
    add_minimal_ontology(ds.default_graph)

    # Optionally load additional ontology files (recommended)
    if args.load_schema and os.path.exists(args.load_schema):
        ds.default_graph.parse(args.load_schema, format="turtle")
    if args.load_taxonomy and os.path.exists(args.load_taxonomy):
        ds.default_graph.parse(args.load_taxonomy, format="turtle")

    def ctx(giri: Optional[URIRef]) -> Graph:
        g = ds.graph(giri) if giri else ds.default_graph
        ensure_prefixes(g)
        return g

    # CSV processing
    with open(args.inp, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required_cols = {"time"}
        missing = required_cols - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {sorted(list(missing))}")

        for row_idx, row in enumerate(reader):
            # Time
            dt = parse_time(row["time"])
            iso_z = time_to_iso_z(dt)
            t_key = time_to_key(dt)
            t_lit = Literal(iso_z, datatype=XSD.dateTime)

            # Slots (A/B/C fixed for your CSV)
            slots = [
                ("A", row.get("BF_object_A"), row.get("latitude_A"), row.get("longitude_A")),
                ("B", row.get("BF_object_B"), row.get("latitude_B"), row.get("longitude_B")),
                ("C", row.get("BF_object_C"), row.get("latitude_C"), row.get("longitude_C")),
            ]

            # --- PositionObservations ---
            for _, obj_id, lat_s, lon_s in slots:
                if not obj_id:
                    continue
                lat_lit = safe_decimal(lat_s)
                lon_lit = safe_decimal(lon_s)
                if lat_lit is None or lon_lit is None:
                    continue

                giri = graph_iri(args.graph_mode, args.file_tag, t_key, obj_id)
                g = ctx(giri)

                platform = STKG[f"platform/{obj_id}"]
                obs = STKG[f"obs/pos/{obj_id}/{t_key}/{row_idx}"]

                g.add((platform, RDF.type, STKG.Platform))

                g.add((obs, RDF.type, STKG.PositionObservation))
                g.add((obs, STKG.observedEntity, platform))
                g.add((obs, STKG.time, t_lit))
                g.add((obs, GEO.lat, lat_lit))
                g.add((obs, GEO.long, lon_lit))

                # provenance
                g.add((obs, STKG.sourceFile, Literal(os.path.basename(args.inp))))
                g.add((obs, STKG.sourceRow, Literal(row_idx, datatype=XSD.integer)))

            # --- SpatialRelationObservations ---
            # (A-B relation) subject A, object B
            # (B-A relation) subject B, object A
            # (C-A relation) subject C, object A
            rel_specs: Tuple[Tuple[str, str, str], ...] = (
                ("BF_object_A", "BF_object_B", "A-B relation"),
                ("BF_object_B", "BF_object_A", "B-A relation"),
                ("BF_object_C", "BF_object_A", "C-A relation"),
            )
            for sub_col, obj_col, rel_col in rel_specs:
                sub_id = row.get(sub_col)
                obj_id = row.get(obj_col)
                rel_raw = row.get(rel_col, "")

                rel_token = normalize_relation(rel_raw, rel_map)
                if not sub_id or not obj_id or not rel_token:
                    continue

                # by_platform이면 subject 기준 graph에 넣는 정책
                platform_for_graph = sub_id if args.graph_mode == "by_platform" else sub_id
                giri = graph_iri(args.graph_mode, args.file_tag, t_key, platform_for_graph)
                g = ctx(giri)

                sub = STKG[f"platform/{sub_id}"]
                obj = STKG[f"platform/{obj_id}"]
                robs = STKG[f"obs/rel/{sub_id}/{obj_id}/{t_key}/{row_idx}"]

                g.add((sub, RDF.type, STKG.Platform))
                g.add((obj, RDF.type, STKG.Platform))

                g.add((robs, RDF.type, STKG.SpatialRelationObservation))
                g.add((robs, STKG.subjectEntity, sub))
                g.add((robs, STKG.objectEntity, obj))
                g.add((robs, STKG.relationType, STKGREL[rel_token]))
                g.add((robs, STKG.time, t_lit))
                g.add((robs, STKG.sourceFile, Literal(os.path.basename(args.inp))))
                g.add((robs, STKG.sourceRow, Literal(row_idx, datatype=XSD.integer)))

    # Serialize
    if args.emit == "ttl":
        out_path = args.out + ".ttl"
        ds.default_graph.serialize(destination=out_path, format="turtle")
    elif args.emit == "trig":
        out_path = args.out + ".trig"
        ds.serialize(destination=out_path, format="trig")
    else:
        out_path = args.out + ".nq"
        ds.serialize(destination=out_path, format="nquads")

    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
