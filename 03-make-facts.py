#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Creates STKG facts from raw CSV observations

Call:
  python3 03-make-facts.py --in input-data/stkg/raw/KG1.csv --out yago-data/03-stkg-facts.tsv

Input:
- raw CSV observation file
- optional relation-map.tsv

Output:
- 03-stkg-facts.tsv

Algorithm:
1) Read CSV rows
2) Detect object slots, coordinates, and relation columns
3) Create PositionObservation facts
4) Create SpatialRelationObservation facts
5) Write all facts as TSV triples
"""

import argparse
import csv
import os
import re
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, List, Set


STKG = "http://example.org/stkg/"
STKGREL = "http://example.org/stkg/relation/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD_DATETIME = "http://www.w3.org/2001/XMLSchema#dateTime"
XSD_DECIMAL = "http://www.w3.org/2001/XMLSchema#decimal"
XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
GEO_LAT = "http://www.w3.org/2003/01/geo/wgs84_pos#lat"
GEO_LONG = "http://www.w3.org/2003/01/geo/wgs84_pos#long"


_TIME_PATTERNS = [
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d",
]


def parse_time(timestr: str) -> datetime:
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


def normalize_rel_fallback(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def load_relation_map_tsv(path: Optional[str]) -> Dict[str, str]:
    if not path:
        return {}

    if not os.path.exists(path):
        raise FileNotFoundError(f"relation map file not found: {path}")

    mapping: Dict[str, str] = {}
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
                mapping[raw.lower()] = norm
    return mapping


def normalize_relation(raw: str, rel_map: Dict[str, str]) -> str:
    r = (raw or "").strip()
    if not r:
        return ""
    hit = rel_map.get(r.lower())
    if hit:
        return hit
    return normalize_rel_fallback(r)


def normalize_header(h: str) -> str:
    if h is None:
        return ""
    return h.replace("\ufeff", "").strip()


def normalize_row_keys(row: Dict[str, str]) -> Dict[str, str]:
    return {normalize_header(k): v for k, v in (row or {}).items()}


_SLOT_RE = re.compile(r"^BF_object_([A-Za-z0-9]+)$")
_REL_RE_1 = re.compile(r"^([A-Za-z0-9]+)\-([A-Za-z0-9]+)$")
_REL_RE_2 = re.compile(r"^([A-Za-z0-9]+)\-([A-Za-z0-9]+)\s*relation$", re.I)


def detect_slots(fieldnames: List[str]) -> List[str]:
    slots: Set[str] = set()
    for h in fieldnames:
        m = _SLOT_RE.match(h)
        if m:
            slots.add(m.group(1))
    return sorted(slots)


def pick_lat_lon_cols(fieldnames: List[str], slot: str) -> Tuple[Optional[str], Optional[str]]:
    candidates_lat = [f"lat_{slot}", f"latitude_{slot}"]
    candidates_lon = [f"long_{slot}", f"longitude_{slot}"]

    lat_col = next((c for c in candidates_lat if c in fieldnames), None)
    lon_col = next((c for c in candidates_lon if c in fieldnames), None)
    return lat_col, lon_col


def detect_relation_cols(fieldnames: List[str]) -> List[Tuple[str, str, str]]:
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


def safe_decimal_lexical(s: str) -> Optional[str]:
    if s is None:
        return None
    ss = str(s).strip()
    if ss == "":
        return None
    try:
        v = float(ss)
        return f"{v:.10f}".rstrip("0").rstrip(".")
    except Exception:
        return None


def uri(local: str) -> str:
    return STKG + local


def rel_uri(token: str) -> str:
    return STKGREL + token


def typed_literal(value: str, datatype_uri: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f"\"{escaped}\"^^<{datatype_uri}>"


def integer_literal(value: int) -> str:
    return f"\"{value}\"^^<{XSD_INTEGER}>"


def fact(s: str, p: str, o: str) -> Tuple[str, str, str]:
    return (s, p, o)


def write_tsv(facts: List[Tuple[str, str, str]], out_path: str) -> None:
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        for s, p, o in facts:
            w.writerow([s, p, o])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="input CSV")
    ap.add_argument("--out", dest="out", required=True, help="output TSV")
    ap.add_argument(
        "--relation_map",
        default="input-data/stkg/relation-map.tsv",
        help="TSV mapping for relation tokens",
    )
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    rel_map = {}
    if args.relation_map and os.path.exists(args.relation_map):
        rel_map = load_relation_map_tsv(args.relation_map)

    all_facts: List[Tuple[str, str, str]] = []

    with open(args.inp, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        raw_fieldnames = reader.fieldnames or []
        fieldnames = [normalize_header(h) for h in raw_fieldnames]
        reader.fieldnames = fieldnames

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
            time_lit = typed_literal(iso_z, XSD_DATETIME)

            # PositionObservation facts
            for slot in slots:
                obj_id = (row.get(f"BF_object_{slot}") or "").strip()
                if not obj_id:
                    continue

                lat_col, lon_col = pick_lat_lon_cols(fieldnames, slot)
                if not lat_col or not lon_col:
                    continue

                lat_lex = safe_decimal_lexical(row.get(lat_col))
                lon_lex = safe_decimal_lexical(row.get(lon_col))
                if lat_lex is None or lon_lex is None:
                    continue

                platform = uri(f"platform/{obj_id}")
                obs = uri(f"obs/pos/{obj_id}/{t_key}/{row_idx}")
                src_file = typed_literal(os.path.basename(args.inp), "http://www.w3.org/2001/XMLSchema#string")
                src_row = integer_literal(row_idx)

                all_facts.extend([
                    fact(platform, RDF_TYPE, uri("Platform")),
                    fact(obs, RDF_TYPE, uri("PositionObservation")),
                    fact(obs, uri("observedEntity"), platform),
                    fact(obs, uri("time"), time_lit),
                    fact(obs, GEO_LAT, typed_literal(lat_lex, XSD_DECIMAL)),
                    fact(obs, GEO_LONG, typed_literal(lon_lex, XSD_DECIMAL)),
                    fact(obs, uri("sourceFile"), src_file),
                    fact(obs, uri("sourceRow"), src_row),
                ])

            # SpatialRelationObservation facts
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

                sub = uri(f"platform/{sub_id}")
                obj = uri(f"platform/{obj_id}")
                robs = uri(
                    f"obs/rel/{sub_id}/{obj_id}/{t_key}/{row_idx}/{normalize_rel_fallback(col_name)}"
                )
                src_file = typed_literal(os.path.basename(args.inp), "http://www.w3.org/2001/XMLSchema#string")
                src_row = integer_literal(row_idx)

                all_facts.extend([
                    fact(sub, RDF_TYPE, uri("Platform")),
                    fact(obj, RDF_TYPE, uri("Platform")),
                    fact(robs, RDF_TYPE, uri("SpatialRelationObservation")),
                    fact(robs, uri("subjectEntity"), sub),
                    fact(robs, uri("objectEntity"), obj),
                    fact(robs, uri("relationType"), rel_uri(rel_token)),
                    fact(robs, uri("time"), time_lit),
                    fact(robs, uri("sourceFile"), src_file),
                    fact(robs, uri("sourceRow"), src_row),
                ])

    write_tsv(all_facts, args.out)
    print(f"Wrote facts: {args.out}")


if __name__ == "__main__":
    main()