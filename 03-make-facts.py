#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Creates STKG facts from one or more raw CSV observation files.

Single-file call:
  python3 03-make-facts.py \
    --in input-data/stkg/raw/KG1.csv \
    --out yago-data/KG1/03-stkg-facts.tsv

Multi-file call (writes one TSV per CSV):
  python3 03-make-facts.py \
    --inputs input-data/stkg/raw/KG1.csv input-data/stkg/raw/KG2.csv input-data/stkg/raw/KG3.csv \
    --outdir yago-data/facts

Input:
- one or more raw CSV observation files
- optional relation-map.tsv

Output:
- single mode: one TSV file
- multi mode: one TSV file per CSV

Algorithm:
1) Read CSV rows
2) Detect object slots, coordinates, relation columns, unary predicate columns
3) Create PositionObservation facts
4) Attach unary predicates/states to PositionObservation if present
5) Create SpatialRelationObservation facts
6) Write all facts as TSV triples
"""

import argparse
import csv
import glob
import os
import re
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, List, Set, Iterable


STKG = "http://example.org/stkg/"
STKGREL = "http://example.org/stkg/relation/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
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

_SLOT_RE = re.compile(r"^BF_object_([A-Za-z0-9]+)$")
_REL_RE_1 = re.compile(r"^([A-Za-z0-9]+)\-([A-Za-z0-9]+)$")
_REL_RE_2 = re.compile(r"^([A-Za-z0-9]+)\-([A-Za-z0-9]+)\s*relation$", re.I)

# relA, relB, relC / state1 / predicate1 / status1 같은 unary predicate 컬럼 탐지
_UNARY_PRED_COL_RE = re.compile(
    r"^(rel[A-Za-z0-9_]*|state[A-Za-z0-9_]*|predicate[A-Za-z0-9_]*|status[A-Za-z0-9_]*)$",
    re.I,
)


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


def detect_slots(fieldnames: List[str]) -> List[str]:
    slots: Set[str] = set()
    for h in fieldnames:
        m = _SLOT_RE.match(h)
        if m:
            slots.add(m.group(1))
    return sorted(slots)


def pick_lat_lon_cols(fieldnames: List[str], slot: str) -> Tuple[Optional[str], Optional[str]]:
    candidates_lat = [f"lat_{slot}", f"latitude_{slot}"]
    candidates_lon = [f"long_{slot}", f"longitude_{slot}", f"lon_{slot}"]

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


def detect_unary_predicate_cols(fieldnames: List[str]) -> List[str]:
    """
    relA, relB, relC / state1 / predicate1 / status1 같은 컬럼을 탐지한다.
    단, A-B / A-B relation 같은 binary relation 컬럼은 제외한다.
    """
    binary_rel_cols = {h for h, _, _ in detect_relation_cols(fieldnames)}
    out: List[str] = []

    for h in fieldnames:
        if h in binary_rel_cols:
            continue
        if h == "time":
            continue
        if _SLOT_RE.match(h):
            continue
        if h.startswith("lat_") or h.startswith("latitude_"):
            continue
        if h.startswith("long_") or h.startswith("longitude_") or h.startswith("lon_"):
            continue
        if _UNARY_PRED_COL_RE.match(h):
            out.append(h)

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
    return f'"{escaped}"^^<{datatype_uri}>'


def integer_literal(value: int) -> str:
    return f'"{value}"^^<{XSD_INTEGER}>'


def fact(s: str, p: str, o: str) -> Tuple[str, str, str]:
    return (s, p, o)


def write_tsv(facts: List[Tuple[str, str, str]], out_path: str) -> None:
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
        for s, p, o in facts:
            w.writerow([s, p, o])


def dedupe_preserve_order(facts: Iterable[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
    seen: Set[Tuple[str, str, str]] = set()
    out: List[Tuple[str, str, str]] = []
    for item in facts:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def dedupe_strings_preserve_order(values: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def split_unary_predicate_values(raw: str) -> List[str]:
    """
    하나의 셀에 여러 상태가 들어오는 경우를 대비.
    기본적으로 ; 또는 | 로 분리.
    공백은 상태 내부("taking cover")에 쓰이므로 separator로 쓰지 않는다.
    """
    s = (raw or "").strip()
    if not s:
        return []
    parts = re.split(r"[;|]+", s)
    return [p.strip() for p in parts if p.strip()]


def csv_to_facts(inp_path: str, rel_map: Dict[str, str]) -> List[Tuple[str, str, str]]:
    all_facts: List[Tuple[str, str, str]] = []

    with open(inp_path, newline="", encoding="utf-8-sig") as f:
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
        unary_pred_cols = detect_unary_predicate_cols(fieldnames)
        src_basename = os.path.basename(inp_path)

        for row_idx, row in enumerate(reader, start=1):
            row = normalize_row_keys(row)

            dt = parse_time(row.get("time", ""))
            iso_z = time_to_iso_z(dt)
            t_key = time_to_key(dt)
            time_lit = typed_literal(iso_z, XSD_DATETIME)

            # -------------------------------------------------
            # 먼저 row 안의 position candidates를 수집
            # -------------------------------------------------
            pos_candidates: List[Tuple[str, str, str, str]] = []
            # (slot, obj_id, lat_lex, lon_lex)

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

                pos_candidates.append((slot, obj_id, lat_lex, lon_lex))

            # -------------------------------------------------
            # unary predicate(relA, relB, relC 등)를 어느 PositionObservation에 붙일지 결정
            # - 기본: row 내 position candidate가 1개면 그 엔티티에 붙임
            # - 여러 개면 모호하므로 붙이지 않음
            # -------------------------------------------------
            unary_target_slots: Set[str] = set()
            if len(pos_candidates) == 1 and unary_pred_cols:
                unary_target_slots.add(pos_candidates[0][0])

            unary_pred_tokens: List[str] = []
            if unary_target_slots:
                raw_values: List[str] = []
                for col in unary_pred_cols:
                    cell = (row.get(col) or "").strip()
                    if not cell:
                        continue
                    raw_values.extend(split_unary_predicate_values(cell))

                unary_pred_tokens = dedupe_strings_preserve_order(
                    [
                        normalize_relation(v, rel_map)
                        for v in raw_values
                        if normalize_relation(v, rel_map)
                    ]
                )

            # -------------------------------------------------
            # PositionObservation facts
            # -------------------------------------------------
            for slot, obj_id, lat_lex, lon_lex in pos_candidates:
                platform = uri(f"platform/{obj_id}")
                obs = uri(f"obs/pos/{obj_id}/{t_key}/{row_idx}")
                src_file = typed_literal(src_basename, XSD_STRING)
                src_row = integer_literal(row_idx)

                row_facts = [
                    fact(platform, RDF_TYPE, uri("Platform")),
                    fact(obs, RDF_TYPE, uri("PositionObservation")),
                    fact(obs, uri("observedEntity"), platform),
                    fact(obs, uri("time"), time_lit),
                    fact(obs, GEO_LAT, typed_literal(lat_lex, XSD_DECIMAL)),
                    fact(obs, GEO_LONG, typed_literal(lon_lex, XSD_DECIMAL)),
                    fact(obs, uri("sourceFile"), src_file),
                    fact(obs, uri("sourceRow"), src_row),
                ]

                # unary predicate를 PositionObservation에 부착
                # 예: moving, not scouting, not taking cover
                if slot in unary_target_slots:
                    for pred_token in unary_pred_tokens:
                        row_facts.append(
                            fact(obs, uri("hasPredicate"), rel_uri(pred_token))
                        )

                all_facts.extend(row_facts)

            # -------------------------------------------------
            # SpatialRelationObservation facts (기존 binary relation)
            # -------------------------------------------------
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
                src_file = typed_literal(src_basename, XSD_STRING)
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

    return dedupe_preserve_order(all_facts)


def expand_inputs(values: List[str]) -> List[str]:
    out: List[str] = []
    for value in values:
        hits = sorted(glob.glob(value))
        if hits:
            out.extend(hits)
        else:
            out.append(value)

    seen: Set[str] = set()
    uniq: List[str] = []
    for p in out:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def out_name_for_csv(csv_path: str) -> str:
    stem = os.path.splitext(os.path.basename(csv_path))[0]
    return f"03-stkg-facts-{stem}.tsv"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", help="single input CSV")
    ap.add_argument("--out", dest="out", help="single output TSV")
    ap.add_argument("--inputs", nargs="+", help="multiple input CSVs or glob patterns")
    ap.add_argument("--outdir", help="output directory for multi-file mode")
    ap.add_argument(
        "--relation_map",
        default="input-data/stkg/relation-map.tsv",
        help="TSV mapping for relation tokens (optional)",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    single_mode = bool(args.inp or args.out)
    multi_mode = bool(args.inputs or args.outdir)

    if single_mode and multi_mode:
        raise ValueError("Use either (--in, --out) or (--inputs, --outdir), not both.")

    if args.inp or args.out:
        if not (args.inp and args.out):
            raise ValueError("Single-file mode requires both --in and --out.")
        input_paths = [args.inp]
        output_paths = [args.out]
    else:
        if not (args.inputs and args.outdir):
            raise ValueError("Multi-file mode requires both --inputs and --outdir.")
        input_paths = expand_inputs(args.inputs)
        if not input_paths:
            raise ValueError("No input CSV files found.")
        os.makedirs(args.outdir, exist_ok=True)
        output_paths = [os.path.join(args.outdir, out_name_for_csv(p)) for p in input_paths]

    rel_map: Dict[str, str] = {}
    if args.relation_map and os.path.exists(args.relation_map):
        rel_map.update(load_relation_map_tsv(args.relation_map))

    for inp_path, out_path in zip(input_paths, output_paths):
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        facts = csv_to_facts(inp_path, rel_map)
        write_tsv(facts, out_path)
        print(f"Wrote facts: {out_path} ({len(facts)} facts)")


if __name__ == "__main__":
    main()