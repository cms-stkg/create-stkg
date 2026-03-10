#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03b-make-stkg.py (patched universal)
CSV(전장/trajectory 관측) -> STKG RDF 생성 (n-ary 필수, named graph 옵션)

지원:
- BF_object_{SLOT} 슬롯 가변 (A/B/C/D/E...)
- lat/long 또는 latitude/longitude 컬럼 자동 인식
- 관계 컬럼: "A-B" 또는 "A-B relation" 자동 인식
- time 컬럼 BOM(﻿time) 자동 처리

예)
python3 03b-make-stkg.py --in KG1.csv --out out/KG1 --graph_mode none --emit trig
python3 03b-make-stkg.py --in KG2.csv --out out/KG2 --graph_mode by_file --emit nq --file_tag KG2
"""

import argparse
import csv
import os
import re
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, List, Set

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

    if s.startswith("+"):
        s = s[1:].strip()

    if s.endswith("Z"):
        s2 = s[:-1]
        try:
            dt = datetime.fromisoformat(s2)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

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
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ===== Relation normalization =====
def normalize_rel_fallback(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s

def load_relation_map_tsv(path: Optional[str]) -> Dict[str, str]:
    """
    TSV format:
    raw_value <TAB> normalized_token
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


def normalize_header(h: str) -> str:
    # strip BOM and whitespace
    if h is None:
        return ""
    return h.replace("\ufeff", "").strip()

def normalize_row_keys(row: Dict[str, str]) -> Dict[str, str]:
    return {normalize_header(k): v for k, v in (row or {}).items()}

_SLOT_RE = re.compile(r"^BF_object_([A-Za-z0-9]+)$")
_REL_RE_1 = re.compile(r"^([A-Za-z0-9]+)\-([A-Za-z0-9]+)$")                 # A-B
_REL_RE_2 = re.compile(r"^([A-Za-z0-9]+)\-([A-Za-z0-9]+)\s*relation$", re.I) # A-B relation

def detect_slots(fieldnames: List[str]) -> List[str]:
    slots: Set[str] = set()
    for h in fieldnames:
        m = _SLOT_RE.match(h)
        if m:
            slots.add(m.group(1))
    return sorted(slots)

def pick_lat_lon_cols(fieldnames: List[str], slot: str) -> Tuple[Optional[str], Optional[str]]:
    # support lat_A / long_A and latitude_A / longitude_A
    candidates_lat = [f"lat_{slot}", f"latitude_{slot}"]
    candidates_lon = [f"long_{slot}", f"longitude_{slot}"]

    lat_col = next((c for c in candidates_lat if c in fieldnames), None)
    lon_col = next((c for c in candidates_lon if c in fieldnames), None)
    return lat_col, lon_col

def detect_relation_cols(fieldnames: List[str]) -> List[Tuple[str, str, str]]:
    """
    Return list of (col_name, sub_slot, obj_slot)
    """
    out: List[Tuple[str, str, str]] = []
    for h in fieldnames:
        m1 = _REL_RE_1.match(h)
        if m1:
            out.append((h, m1.group(1), m1.group(2)))
            continue
        m2 = _REL_RE_2.match(h)
        if m2:
            out.append((h, m2.group(1), m2.group(2)))
            continue
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="input CSV")
    ap.add_argument("--out", dest="out", required=True, help="output path prefix (no extension)")
    ap.add_argument("--emit", choices=["ttl", "trig", "nq"], default="trig", help="output format")
    ap.add_argument("--graph_mode", choices=["none", "by_file", "by_time", "by_platform"], default="none")
    ap.add_argument("--file_tag", default="KG1", help="tag for by_file graph IRI")
    ap.add_argument("--relation_map", default="input-data/stkg/relation-map.tsv", help="TSV mapping for relation tokens")
    ap.add_argument("--load_schema", default="input-data/stkg/stkg-schema.ttl", help="optional schema ttl")
    ap.add_argument("--load_taxonomy", default="input-data/stkg/stkg-taxonomy.ttl", help="optional taxonomy ttl")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    rel_map = {}
    if args.relation_map and os.path.exists(args.relation_map):
        rel_map = load_relation_map_tsv(args.relation_map)

    ds = Dataset()
    ensure_prefixes(ds.default_graph)
    add_minimal_ontology(ds.default_graph)

    if args.load_schema and os.path.exists(args.load_schema):
        ds.default_graph.parse(args.load_schema, format="turtle")
    if args.load_taxonomy and os.path.exists(args.load_taxonomy):
        ds.default_graph.parse(args.load_taxonomy, format="turtle")

    def ctx(giri: Optional[URIRef]) -> Graph:
        g = ds.graph(giri) if giri else ds.default_graph
        ensure_prefixes(g)
        return g

    with open(args.inp, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # normalize fieldnames (handle BOM in "time")
        raw_fieldnames = reader.fieldnames or []
        fieldnames = [normalize_header(h) for h in raw_fieldnames]
        reader.fieldnames = fieldnames  # ensures rows come with normalized keys

        required_cols = {"time"}
        missing = required_cols - set(fieldnames)
        if missing:
            raise ValueError(f"CSV missing required columns: {sorted(list(missing))}")

        slots = detect_slots(fieldnames)
        rel_cols = detect_relation_cols(fieldnames)

        for row_idx, row in enumerate(reader):
            row = normalize_row_keys(row)

            dt = parse_time(row.get("time", ""))
            iso_z = time_to_iso_z(dt)
            t_key = time_to_key(dt)
            t_lit = Literal(iso_z, datatype=XSD.dateTime)

            # --- PositionObservations (all slots) ---
            for slot in slots:
                obj_id = (row.get(f"BF_object_{slot}") or "").strip()
                if not obj_id:
                    continue

                lat_col, lon_col = pick_lat_lon_cols(fieldnames, slot)
                if not lat_col or not lon_col:
                    continue

                lat_lit = safe_decimal(row.get(lat_col))
                lon_lit = safe_decimal(row.get(lon_col))
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

                g.add((obs, STKG.sourceFile, Literal(os.path.basename(args.inp))))
                g.add((obs, STKG.sourceRow, Literal(row_idx, datatype=XSD.integer)))

            # --- SpatialRelationObservations (all relation columns like A-B / A-B relation) ---
            for col_name, sub_slot, obj_slot in rel_cols:
                rel_raw = (row.get(col_name) or "").strip()
                if not rel_raw:
                    continue

                sub_id = (row.get(f"BF_object_{sub_slot}") or "").strip()
                obj_id = (row.get(f"BF_object_{obj_slot}") or "").strip()
                if not sub_id or not obj_id:
                    continue

                rel_token = normalize_relation(rel_raw, rel_map)
                if not rel_token:
                    continue

                # graph placement policy: by_platform uses subject id
                platform_for_graph = sub_id if args.graph_mode == "by_platform" else sub_id
                giri = graph_iri(args.graph_mode, args.file_tag, t_key, platform_for_graph)
                g = ctx(giri)

                sub = STKG[f"platform/{sub_id}"]
                obj = STKG[f"platform/{obj_id}"]
                robs = STKG[f"obs/rel/{sub_id}/{obj_id}/{t_key}/{row_idx}/{normalize_rel_fallback(col_name)}"]

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