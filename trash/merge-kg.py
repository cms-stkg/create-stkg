#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
merge-stkg-kg1-kg2.py (with conflict resolution / dedup)

- KG1(.nq/.trig/.ttl) + KG2(.nq/.trig/.ttl) 로드
- 표기 이질성(엔티티/관계) 해결 + 충돌 해결(중복 제거) 수행
- 출력: merged.nq / merged.trig / merged.ttl

핵심:
- 엔티티 canonical: stkg:entity/<normalized_key>
- 엔티티 정렬: normalized_key가 같으면 owl:sameAs
- 관계 canonical: seed mapping으로 토큰 통일 (+ optional string similarity 보조)
- Observation을 "키 기반 canonical observation URI"로 재구성 => dedup/merge가 확실히 됨
- provenance(sourceFile/sourceRow)는 중복 관측이 합쳐질 때 모두 보존(다중값)
"""

import argparse
import os
import re
import hashlib
from difflib import SequenceMatcher
from typing import Dict, Optional, Set, Tuple, List, Iterable, DefaultDict
from collections import defaultdict

from rdflib import Dataset, Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD, OWL

STKG = Namespace("http://example.org/stkg/")
STKGREL = Namespace("http://example.org/stkg/relation/")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

# ---- Canonical relation tokens ----
CANONICAL_REL = {
    "front": "front_of",
    "back": "back_of",
    "side": "side_by_side",
}

REL_SEED_TO_CANONICAL = {
    "in_front_of": CANONICAL_REL["front"],
    "ahead_of": CANONICAL_REL["front"],

    "in_back_of": CANONICAL_REL["back"],
    "behind": CANONICAL_REL["back"],

    "side_by_side": CANONICAL_REL["side"],
    "beside": CANONICAL_REL["side"],
    "next_to": CANONICAL_REL["side"],  # (KG3 대비)
}

def first_obj(dataset: Dataset, s: URIRef, p: URIRef):
    for _s, _p, o, _g in dataset.quads((s, p, None, None)):
        return o
    return None

def all_objs(dataset: Dataset, s: URIRef, p: URIRef):
    out = []
    for _s, _p, o, _g in dataset.quads((s, p, None, None)):
        out.append(o)
    return out

def norm_entity_key(raw_id: str) -> str:
    s = (raw_id or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def extract_platform_id(uri: URIRef) -> Optional[str]:
    us = str(uri)
    marker = "/platform/"
    if marker in us:
        return us.split(marker, 1)[1]
    return None

def extract_rel_token(uri: URIRef) -> Optional[str]:
    us = str(uri)
    marker = str(STKGREL)
    if us.startswith(marker):
        return us[len(marker):]
    return None

def best_effort_relation_to_canonical(token: str, all_known: Set[str], thresh: float = 0.82) -> Optional[str]:
    if not token:
        return None
    t = token.strip().lower()
    if t in REL_SEED_TO_CANONICAL:
        return REL_SEED_TO_CANONICAL[t]

    best = None
    best_score = 0.0
    for cand in all_known:
        score = SequenceMatcher(None, t, cand).ratio()
        if score > best_score:
            best_score = score
            best = cand
    if best and best_score >= thresh and best in REL_SEED_TO_CANONICAL:
        return REL_SEED_TO_CANONICAL[best]
    return None

def guess_rdf_format(path: str) -> str:
    ext = os.path.splitext(path.lower())[1]
    if ext in [".nq", ".nquads"]:
        return "nquads"
    if ext in [".trig"]:
        return "trig"
    if ext in [".ttl", ".turtle"]:
        return "turtle"
    return "nquads"

def bind_prefixes(g: Graph) -> None:
    g.bind("stkg", STKG)
    g.bind("stkgrel", STKGREL)
    g.bind("geo", GEO)
    g.bind("owl", OWL)
    g.bind("rdf", RDF)
    g.bind("xsd", XSD)

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def _lit_to_str(l: Optional[Literal]) -> str:
    if l is None:
        return ""
    return str(l)

def _as_float(l: Optional[Literal]) -> Optional[float]:
    if l is None:
        return None
    try:
        return float(str(l))
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kg1", required=True, help="path to KG1 (.nq/.trig/.ttl)")
    ap.add_argument("--kg2", required=True, help="path to KG2 (.nq/.trig/.ttl)")
    ap.add_argument("--out", required=True, help="output path prefix (no extension)")
    ap.add_argument("--emit", choices=["nq", "trig", "ttl"], default="nq")
    ap.add_argument("--merged_graph", default="http://example.org/stkg/graph/file/MERGED_KG1_KG2",
                    help="Named graph URI for merged canonical triples")
    ap.add_argument("--keep_sources", action="store_true",
                    help="If set, keep original source graphs as-is in output dataset too")

    # --- conflict/dedup controls ---
    ap.add_argument("--pos_dedup", choices=["strict", "by_time"], default="strict",
                    help="PositionObservation dedup key: strict=(entity,time,lat,long) / by_time=(entity,time)")
    ap.add_argument("--pos_conflict_policy", choices=["keep_first", "average", "keep_all"], default="keep_first",
                    help="When pos_dedup=by_time and lat/long differ: keep_first / average / keep_all")
    args = ap.parse_args()

    # ---- Load KGs ----
    ds1 = Dataset()
    ds2 = Dataset()
    bind_prefixes(ds1.default_graph)
    bind_prefixes(ds2.default_graph)
    ds1.parse(args.kg1, format=guess_rdf_format(args.kg1))
    ds2.parse(args.kg2, format=guess_rdf_format(args.kg2))

    # ---- Collect platform URIs ----
    platform_uris: Set[URIRef] = set()
    for ds in (ds1, ds2):
        for s, p, o, g in ds.quads((None, RDF.type, STKG.Platform, None)):
            platform_uris.add(s)

    # Build map: original platform URI -> canonical entity URI
    uri_to_canon: Dict[URIRef, URIRef] = {}
    key_to_canon: Dict[str, URIRef] = {}
    for pu in platform_uris:
        pid = extract_platform_id(pu)
        if not pid:
            continue
        key = norm_entity_key(pid)
        if key not in key_to_canon:
            key_to_canon[key] = STKG[f"entity/{key}"]
        uri_to_canon[pu] = key_to_canon[key]

    # ---- Collect relation tokens ----
    rel_tokens: Set[str] = set()
    for ds in (ds1, ds2):
        for s, p, o, g in ds.quads((None, STKG.relationType, None, None)):
            if isinstance(o, URIRef):
                tok = extract_rel_token(o)
                if tok:
                    rel_tokens.add(tok.lower())

    rel_uri_to_canon: Dict[URIRef, URIRef] = {}
    for tok in rel_tokens:
        canon_tok = best_effort_relation_to_canonical(tok, rel_tokens) or tok
        rel_uri_to_canon[STKGREL[tok]] = STKGREL[canon_tok]

    def rewrite_term(t):
        if isinstance(t, URIRef):
            if t in uri_to_canon:
                return uri_to_canon[t]
            if str(t).startswith(str(STKGREL)) and t in rel_uri_to_canon:
                return rel_uri_to_canon[t]
        return t

    # ---- Output dataset ----
    out_ds = Dataset()
    bind_prefixes(out_ds.default_graph)
    merged_g_uri = URIRef(args.merged_graph)
    merged_g = out_ds.graph(merged_g_uri)
    bind_prefixes(merged_g)

    # owl:sameAs in default graph (keeping design)
    for orig, canon in uri_to_canon.items():
        out_ds.default_graph.add((canon, OWL.sameAs, orig))

    # Helper to add multi-provenance
    def add_prov(obs_uri: URIRef, source_files: Set[str], source_rows: Set[str]):
        for sf in sorted(source_files):
            merged_g.add((obs_uri, STKG.sourceFile, Literal(sf)))
        for sr in sorted(source_rows, key=lambda x: int(x) if x.isdigit() else x):
            # keep as integer if possible
            if sr.isdigit():
                merged_g.add((obs_uri, STKG.sourceRow, Literal(int(sr), datatype=XSD.integer)))
            else:
                merged_g.add((obs_uri, STKG.sourceRow, Literal(sr)))

    # ---- Collect & merge PositionObservations and RelationObservations ----
    # We rebuild canonical observation URIs so dedup is robust.

    # Position storage
    # strict key: (entity, time, lat, long)
    seen_pos_strict: Dict[Tuple[str, str, str, str], URIRef] = {}
    pos_prov_strict: DefaultDict[URIRef, Tuple[Set[str], Set[str]]] = defaultdict(lambda: (set(), set()))

    # by_time key: (entity, time) -> list of candidates
    # store candidates so we can average if needed
    pos_candidates_by_time: DefaultDict[Tuple[str, str], List[Tuple[Optional[float], Optional[float], Set[str], Set[str]]]] = defaultdict(list)

    # Relation storage (dedup exact)
    seen_rel: Dict[Tuple[str, str, str, str], URIRef] = {}
    rel_prov: DefaultDict[URIRef, Tuple[Set[str], Set[str]]] = defaultdict(lambda: (set(), set()))

    def collect_from(ds: Dataset):
        # --- PositionObservations ---
        for obs, _, _, _g in ds.quads((None, RDF.type, STKG.PositionObservation, None)):
            # required fields
            ent0 = first_obj(ds, obs, STKG.observedEntity)
            t0   = first_obj(ds, obs, STKG.time)
            lat0 = first_obj(ds, obs, GEO.lat)
            lon0 = first_obj(ds, obs, GEO.long)

            sfiles = set(str(x) for x in all_objs(ds, obs, STKG.sourceFile))
            srows  = set(str(x) for x in all_objs(ds, obs, STKG.sourceRow))

            if not isinstance(ent0, URIRef) or t0 is None or lat0 is None or lon0 is None:
                continue

            ent = rewrite_term(ent0)
            # time literal normalized as string
            t_str = _lit_to_str(t0)

            lat_str = _lit_to_str(lat0)
            lon_str = _lit_to_str(lon0)

            # provenance
            # sfiles = set(str(x) for x in ds.objects(obs, STKG.sourceFile))
            # srows = set(str(x) for x in ds.objects(obs, STKG.sourceRow))

            ent_str = str(ent)

            if args.pos_dedup == "strict":
                key = (ent_str, t_str, lat_str, lon_str)
                if key not in seen_pos_strict:
                    oid = _sha1("|".join(key))
                    canon_obs = STKG[f"obs/pos/{oid}"]
                    seen_pos_strict[key] = canon_obs

                    merged_g.add((canon_obs, RDF.type, STKG.PositionObservation))
                    merged_g.add((canon_obs, STKG.observedEntity, ent))
                    merged_g.add((canon_obs, STKG.time, t0))
                    merged_g.add((canon_obs, GEO.lat, lat0))
                    merged_g.add((canon_obs, GEO.long, lon0))
                # merge provenance
                prov_files, prov_rows = pos_prov_strict[seen_pos_strict[key]]
                prov_files.update(sfiles)
                prov_rows.update(srows)

            else:
                # by_time conflict handling later
                key2 = (ent_str, t_str)
                pos_candidates_by_time[key2].append((
                    _as_float(lat0), _as_float(lon0),
                    set(sfiles), set(srows)
                ))

        # --- SpatialRelationObservations ---
        for robs, _, _, _g in ds.quads((None, RDF.type, STKG.SpatialRelationObservation, None)):
            # def first_obj(dataset: Dataset, s: URIRef, p: URIRef):
            #     for _s, _p, o, _g in dataset.quads((s, p, None, None)):
            #         return o
            #     return None

            sub0 = first_obj(ds, robs, STKG.subjectEntity)
            obj0 = first_obj(ds, robs, STKG.objectEntity)
            rel0 = first_obj(ds, robs, STKG.relationType)
            t0   = first_obj(ds, robs, STKG.time)

            if not isinstance(sub0, URIRef) or not isinstance(obj0, URIRef) or not isinstance(rel0, URIRef) or t0 is None:
                continue

            sub = rewrite_term(sub0)
            obj = rewrite_term(obj0)
            rel = rewrite_term(rel0)

            key = (str(sub), str(obj), _lit_to_str(t0), str(rel))

            # def all_objs(dataset: Dataset, s: URIRef, p: URIRef):
            #     out = []
            #     for _s, _p, o, _g in dataset.quads((s, p, None, None)):
            #         out.append(o)
            #     return out
            
            sfiles = set(str(x) for x in all_objs(ds, robs, STKG.sourceFile))
            srows  = set(str(x) for x in all_objs(ds, robs, STKG.sourceRow))

            if key not in seen_rel:
                oid = _sha1("|".join(key))
                canon_robs = STKG[f"obs/rel/{oid}"]
                seen_rel[key] = canon_robs

                merged_g.add((canon_robs, RDF.type, STKG.SpatialRelationObservation))
                merged_g.add((canon_robs, STKG.subjectEntity, sub))
                merged_g.add((canon_robs, STKG.objectEntity, obj))
                merged_g.add((canon_robs, STKG.relationType, rel))
                merged_g.add((canon_robs, STKG.time, t0))

            prov_files, prov_rows = rel_prov[seen_rel[key]]
            prov_files.update(sfiles)
            prov_rows.update(srows)

    collect_from(ds1)
    collect_from(ds2)

    # Finalize by_time PositionObservations (if enabled)
    if args.pos_dedup == "by_time":
        for (ent_str, t_str), cands in pos_candidates_by_time.items():
            # policy:
            # keep_first -> first candidate
            # average -> average numeric lat/long (ignoring None)
            # keep_all -> create one obs per candidate (still merges exact duplicates if repeated)
            ent_uri = URIRef(ent_str)

            if args.pos_conflict_policy == "keep_all":
                for lat_f, lon_f, sfiles, srows in cands:
                    if lat_f is None or lon_f is None:
                        continue
                    key_full = (ent_str, t_str, str(lat_f), str(lon_f))
                    oid = _sha1("|".join(key_full))
                    canon_obs = STKG[f"obs/pos/{oid}"]

                    merged_g.add((canon_obs, RDF.type, STKG.PositionObservation))
                    merged_g.add((canon_obs, STKG.observedEntity, ent_uri))
                    merged_g.add((canon_obs, STKG.time, Literal(t_str, datatype=XSD.dateTime)))
                    merged_g.add((canon_obs, GEO.lat, Literal(f"{lat_f:.10f}".rstrip("0").rstrip("."), datatype=XSD.decimal)))
                    merged_g.add((canon_obs, GEO.long, Literal(f"{lon_f:.10f}".rstrip("0").rstrip("."), datatype=XSD.decimal)))

                    add_prov(canon_obs, sfiles, srows)

            elif args.pos_conflict_policy == "average":
                lats = [x[0] for x in cands if x[0] is not None]
                lons = [x[1] for x in cands if x[1] is not None]
                if not lats or not lons:
                    continue
                lat_avg = sum(lats) / len(lats)
                lon_avg = sum(lons) / len(lons)

                oid = _sha1("|".join([ent_str, t_str, "avg"]))
                canon_obs = STKG[f"obs/pos/{oid}"]

                merged_g.add((canon_obs, RDF.type, STKG.PositionObservation))
                merged_g.add((canon_obs, STKG.observedEntity, ent_uri))
                merged_g.add((canon_obs, STKG.time, Literal(t_str, datatype=XSD.dateTime)))
                merged_g.add((canon_obs, GEO.lat, Literal(f"{lat_avg:.10f}".rstrip("0").rstrip("."), datatype=XSD.decimal)))
                merged_g.add((canon_obs, GEO.long, Literal(f"{lon_avg:.10f}".rstrip("0").rstrip("."), datatype=XSD.decimal)))

                all_files: Set[str] = set()
                all_rows: Set[str] = set()
                for _, _, sf, sr in cands:
                    all_files.update(sf)
                    all_rows.update(sr)
                add_prov(canon_obs, all_files, all_rows)

            else:
                # keep_first
                lat_f, lon_f, sfiles, srows = cands[0]
                if lat_f is None or lon_f is None:
                    continue
                oid = _sha1("|".join([ent_str, t_str, "first"]))
                canon_obs = STKG[f"obs/pos/{oid}"]

                merged_g.add((canon_obs, RDF.type, STKG.PositionObservation))
                merged_g.add((canon_obs, STKG.observedEntity, ent_uri))
                merged_g.add((canon_obs, STKG.time, Literal(t_str, datatype=XSD.dateTime)))
                merged_g.add((canon_obs, GEO.lat, Literal(f"{lat_f:.10f}".rstrip("0").rstrip("."), datatype=XSD.decimal)))
                merged_g.add((canon_obs, GEO.long, Literal(f"{lon_f:.10f}".rstrip("0").rstrip("."), datatype=XSD.decimal)))

                # provenance는 전부 합쳐주는 편이 보통 유리 (keep_first라도 "근거"는 남겨둠)
                all_files: Set[str] = set()
                all_rows: Set[str] = set()
                for _, _, sf, sr in cands:
                    all_files.update(sf)
                    all_rows.update(sr)
                add_prov(canon_obs, all_files, all_rows)

    # Finalize provenance for strict mode PositionObservations
    if args.pos_dedup == "strict":
        for _key, obs_uri in seen_pos_strict.items():
            files, rows = pos_prov_strict[obs_uri]
            add_prov(obs_uri, files, rows)

    # Finalize provenance for relations
    for _key, robs_uri in seen_rel.items():
        files, rows = rel_prov[robs_uri]
        add_prov(robs_uri, files, rows)

    # Add canonical Platform typing (optional but useful for queries)
    # (So you can query canonical entities as Platforms, even though they were entities originally)
    for canon_ent in set(uri_to_canon.values()):
        merged_g.add((canon_ent, RDF.type, STKG.Platform))

    # optionally keep original graphs too
    if args.keep_sources:
        for src in (ds1, ds2):
            for s, p, o, g in src.quads((None, None, None, None)):
                out_ds.add((s, p, o, g))

    # ---- Serialize ----
    if args.emit == "nq":
        out_path = args.out + ".nq"
        out_ds.serialize(destination=out_path, format="nquads")
    elif args.emit == "trig":
        out_path = args.out + ".trig"
        out_ds.serialize(destination=out_path, format="trig")
    else:
        out_path = args.out + ".ttl"
        merged_g.serialize(destination=out_path, format="turtle")

    # ---- Summary ----
    print(f"Wrote: {out_path}")
    print(f"Merged graph: {args.merged_graph}")
    print(f"Canonical entities: {len(set(uri_to_canon.values()))}")
    print(f"Aligned platforms: {len(uri_to_canon)}")
    print(f"Relation tokens observed: {sorted(rel_tokens)}")
    if args.pos_dedup == "strict":
        print(f"PositionObs (unique): {len(seen_pos_strict)}")
    else:
        print(f"PositionObs (by_time keys): {len(pos_candidates_by_time)}")
        print(f"RelationObs (unique): {len(seen_rel)}")

if __name__ == "__main__":
    main()