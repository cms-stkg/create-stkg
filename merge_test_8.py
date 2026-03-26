#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import csv
from collections import defaultdict, Counter
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD, OWL

# =========================
# 설정
# =========================
KG1_FILE = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data10\\KG1\\stkg-final.ttl"
KG2_FILE = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data10\\KG2\\stkg-final.ttl"
OUT_FILE = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data10\\output\\merge.ttl"
MATCHED_POSITION_PAIR_CSV = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data10\\output\\merged_position_pairs_.csv"

# 센서 오차 허용 범위
# 0 -> 더 엄격(약 0.5m 수준), 1 -> 약 1m 수준
SENSOR_ERROR_TOLERANCE = 1
sensorErrorTol = [2, 1]

LAT_TOL = (5.41e-5 / 6) / sensorErrorTol[SENSOR_ERROR_TOLERANCE]
LONG_TOL = (8.78e-5 / 8) / sensorErrorTol[SENSOR_ERROR_TOLERANCE]

# =========================
# Namespace
# =========================
STKG = Namespace("http://example.org/stkg/")
STKGREL = Namespace("http://example.org/stkg/relation/")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

# 새 KG 생성 파이프라인 기준: 클래스는 stkg: namespace
POSITION_OBS = STKG.PositionObservation
RELATION_OBS = STKG.SpatialRelationObservation
PLATFORM = STKG.Platform

HAS_PREDICATE = STKG.hasPredicate
RELATION_TYPE_PROP = STKG.relationType

# 관계 observation 내부 속성 후보
RELATION_VALUE_PROPS = [
    STKG.relation,
    STKG.spatialRelation,
    STKG.relationType,
]

RELATION_SUBJECT_PROPS = [
    STKG.subjectEntity,
    STKG.sourceEntity,
    STKG.observedEntity1,
]

RELATION_OBJECT_PROPS = [
    STKG.objectEntity,
    STKG.targetEntity,
    STKG.observedEntity2,
]

# =========================
# canonical_set
# - 대표 URI 선택 / 보조 정렬용
# - 이제 "후보 제한" 용도로는 사용하지 않음
# =========================
CANONICAL_ENTITY_SETS = {
    "entity_t72": {"T-72", "T72"},
    "entity_t62": {"T-62", "T62"},
    "entity_btr80": {"BTR-80", "BTR80"},
    "entity_k2blackpanther": {"K2 Black Panther", "K2BlackPanther", "K2_Black_Panther"},
    "entity_t90": {"T-90", "T90"},
}

CANONICAL_PREDICATE_SETS = {
    "inFrontOf": {
        "in_front_of", "ahead_of", "located_ahead_of", "in front of"
    },
    "behind": {
        "in_back_of", "behind", "behind_of", "located_behind_of", "behind of"
    },
    "sideBySide": {
        "side_by_side", "beside", "next_to"
    },
}

ENTITY_ALIAS_TO_CANONICAL = {}
for canonical_id, alias_set in CANONICAL_ENTITY_SETS.items():
    for alias in alias_set:
        norm_alias = re.sub(r"[-_\s]", "", alias.strip().lower())
        ENTITY_ALIAS_TO_CANONICAL[norm_alias] = canonical_id


def export_matched_position_pairs_to_csv(matched_pairs, out_csv):
    """
    시공간 정렬로 매칭된 위치 관측 쌍을 CSV로 저장.
    한 행이 (KG1 position obs, KG2 position obs) 한 쌍을 의미.
    """
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)

        writer.writerow([
            "time",
            "kg1_obs_uri",
            "kg1_entity",
            "kg1_source_file",
            "kg1_source_row",
            "kg1_lat",
            "kg1_long",
            "kg2_obs_uri",
            "kg2_entity",
            "kg2_source_file",
            "kg2_source_row",
            "kg2_lat",
            "kg2_long",
            "lat_diff",
            "long_diff",
            "distance_score"
        ])

        for r1, r2 in matched_pairs:
            lat_diff = abs(r1["lat_val"] - r2["lat_val"])
            long_diff = abs(r1["long_val"] - r2["long_val"])
            dist_score = spatial_distance_score(r1, r2)

            writer.writerow([
                r1["time"],
                str(r1["obs"]),
                str(r1["observed_entity"]),
                str(r1["source_file"]) if r1["source_file"] is not None else "",
                str(r1["source_row"]) if r1["source_row"] is not None else "",
                r1["lat_val"],
                r1["long_val"],
                str(r2["obs"]),
                str(r2["observed_entity"]),
                str(r2["source_file"]) if r2["source_file"] is not None else "",
                str(r2["source_row"]) if r2["source_row"] is not None else "",
                r2["lat_val"],
                r2["long_val"],
                lat_diff,
                long_diff,
                dist_score
            ])


# =========================
# 유틸
# =========================
def detect_rdf_format(filepath: str):
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".ttl":
        return "turtle"
    elif ext == ".nt":
        return "nt"
    else:
        raise ValueError(f"Unsupported RDF file extension: {ext}. Only .ttl and .nt are supported.")


def parse_graph(filepath: str):
    fmt = detect_rdf_format(filepath)
    g = Graph()
    g.parse(filepath, format=fmt)
    return g


def normalize_text(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    s = re.sub(r"[-_\s]", "", s)
    return s


def normalize_relation_surface(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    return s


def get_local_name(uri_or_value):
    if uri_or_value is None:
        return None
    s = str(uri_or_value)
    if "#" in s:
        return s.split("#")[-1]
    return s.rstrip("/").split("/")[-1]


def normalize_entity_name(entity_uri: URIRef):
    if entity_uri is None:
        return None
    name = get_local_name(entity_uri)
    if name is None:
        return None
    # 새 KG는 .../id/platform_BTR-80 형태라 접두어 제거
    if name.startswith("platform_"):
        name = name[len("platform_"):]
    return normalize_text(name)


def get_entity_canonical_id(entity_uri: URIRef):
    norm = normalize_entity_name(entity_uri)
    if norm is None:
        return None
    return ENTITY_ALIAS_TO_CANONICAL.get(norm)


def canonicalize_entity_name(entity_uri: URIRef):
    canonical_id = get_entity_canonical_id(entity_uri)
    if canonical_id is None:
        return None
    return URIRef(f"http://example.org/stkg/id/{canonical_id}")


def canonicalize_predicate(raw_value):
    """
    relationType가 URI(stkg/relation/...)로 들어와도 local name 기준으로 정규화.
    """
    if raw_value is None:
        return None

    s = str(raw_value)
    if s.startswith("http://") or s.startswith("https://"):
        s = get_local_name(s)

    norm = normalize_relation_surface(s)
    if norm is None:
        return None

    for canonical_pred, alias_set in CANONICAL_PREDICATE_SETS.items():
        normalized_aliases = {normalize_relation_surface(a) for a in alias_set}
        if norm in normalized_aliases:
            return canonical_pred

    return norm


def normalize_position_predicate_uri(raw_value):
    """
    hasPredicate 값을 병합용으로 정규화해서 URIRef로 반환.
    - 이미 stkg/relation/... URI면 그대로 사용
    - 아니면 canonical token으로 바꿔 stkg/relation/... URI 생성
    """
    if raw_value is None:
        return None

    s = str(raw_value)
    if s.startswith(str(STKGREL)):
        return URIRef(s)

    token = canonicalize_predicate(raw_value)
    if token is None:
        return None

    return URIRef(f"{str(STKGREL)}{token}")


def value_first(graph, subj, predicates):
    for p in predicates:
        v = graph.value(subj, p)
        if v is not None:
            return v
    return None


def safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


def safe_time_key(v):
    if v is None:
        return None
    return str(v)


def decimal_literal(v: float):
    lexical = f"{v:.10f}".rstrip("0").rstrip(".")
    if lexical == "":
        lexical = "0"
    return Literal(lexical, datatype=XSD.decimal)


def within_spatiotemporal_tolerance(rec1, rec2):
    """
    엔티티 이름을 보지 않고 시공간만으로 비교
    """
    if rec1["time"] != rec2["time"]:
        return False

    lat_ok = abs(rec1["lat_val"] - rec2["lat_val"]) <= LAT_TOL
    long_ok = abs(rec1["long_val"] - rec2["long_val"]) <= LONG_TOL
    return lat_ok and long_ok


def spatial_distance_score(rec1, rec2):
    return abs(rec1["lat_val"] - rec2["lat_val"]) + abs(rec1["long_val"] - rec2["long_val"])


def should_emit_same_as(original_entity, representative_entity):
    """
    owl:sameAs는 다음 경우에만 생성:
    - canonical alias가 같은 경우
    - 또는 시공간 정렬 결과로 실제 같은 representative에 매핑된 경우
    단, 자기 자신 sameAs는 생성하지 않음.
    """
    if original_entity is None or representative_entity is None:
        return False

    if str(original_entity) == str(representative_entity):
        return False

    return True


# =========================
# 시간 범위 계산
# =========================
def get_time_range(records):
    times = [r["time"] for r in records if r["time"] is not None]
    if not times:
        return None, None
    return min(times), max(times)


def compute_overlap_range(records1, records2):
    min1, max1 = get_time_range(records1)
    min2, max2 = get_time_range(records2)

    if min1 is None or min2 is None:
        return None, None

    start = max(min1, min2)
    end = min(max1, max2)

    if start > end:
        return None, None

    return start, end


def is_in_overlap(time_value, overlap_start, overlap_end):
    if overlap_start is None or overlap_end is None:
        return False
    return overlap_start <= time_value <= overlap_end


# =========================
# observation 추출
# =========================
def extract_position_observations(graph: Graph, kg_label: str):
    records = []

    for obs in graph.subjects(RDF.type, POSITION_OBS):
        observed_entity = graph.value(obs, STKG.observedEntity)
        source_file = graph.value(obs, STKG.sourceFile)
        source_row = graph.value(obs, STKG.sourceRow)
        time = graph.value(obs, STKG.time)
        lat = graph.value(obs, GEO.lat)
        lon = graph.value(obs, GEO.long)

        if time is None or lat is None or lon is None or observed_entity is None:
            continue

        lat_val = safe_float(lat)
        lon_val = safe_float(lon)

        if lat_val is None or lon_val is None:
            continue

        predicates = set()
        for p in graph.objects(obs, HAS_PREDICATE):
            norm_p = normalize_position_predicate_uri(p)
            if norm_p is not None:
                predicates.add(norm_p)

        records.append({
            "kg": kg_label,
            "obs": obs,
            "obs_type": "position",
            "observed_entity": observed_entity,
            "source_file": source_file,
            "source_row": source_row,
            "time": safe_time_key(time),
            "time_literal": time,
            "lat": lat,
            "long": lon,
            "lat_val": lat_val,
            "long_val": lon_val,
            "predicates": predicates,
        })

    return records


def extract_relation_observations(graph: Graph, kg_label: str):
    """
    관계 술어 정규화는 시공간 정렬 전에 먼저 수행
    """
    records = []

    for obs in graph.subjects(RDF.type, RELATION_OBS):
        ent1 = value_first(graph, obs, RELATION_SUBJECT_PROPS)
        ent2 = value_first(graph, obs, RELATION_OBJECT_PROPS)
        rel_val = value_first(graph, obs, RELATION_VALUE_PROPS)

        source_file = graph.value(obs, STKG.sourceFile)
        source_row = graph.value(obs, STKG.sourceRow)
        time = graph.value(obs, STKG.time)

        if time is None or ent1 is None or ent2 is None or rel_val is None:
            continue

        rel_token = canonicalize_predicate(rel_val)
        if rel_token is None:
            continue

        records.append({
            "kg": kg_label,
            "obs": obs,
            "obs_type": "relation",
            "subject_entity": ent1,
            "object_entity": ent2,
            "relation_value": rel_val,
            "canonical_relation": rel_token,
            "source_file": source_file,
            "source_row": source_row,
            "time": safe_time_key(time),
            "time_literal": time,
        })

    return records


# =========================
# Stage 1: 시공간 기반 위치 매칭
# - 엔티티 이름 미사용
# - cross-KG끼리만 매칭
# =========================
def build_spatiotemporal_position_matches(kg1_records, kg2_records):
    overlap_start, overlap_end = compute_overlap_range(kg1_records, kg2_records)

    kg1_by_time = defaultdict(list)
    kg2_by_time = defaultdict(list)

    for rec in kg1_records:
        if is_in_overlap(rec["time"], overlap_start, overlap_end):
            kg1_by_time[rec["time"]].append(rec)

    for rec in kg2_records:
        if is_in_overlap(rec["time"], overlap_start, overlap_end):
            kg2_by_time[rec["time"]].append(rec)

    matched_pairs = []
    all_times = sorted(set(kg1_by_time.keys()) | set(kg2_by_time.keys()))

    for t in all_times:
        recs1 = kg1_by_time.get(t, [])
        recs2 = kg2_by_time.get(t, [])

        used_kg2 = set()

        for i, r1 in enumerate(recs1):
            best_j = None
            best_score = None

            for j, r2 in enumerate(recs2):
                if j in used_kg2:
                    continue

                if within_spatiotemporal_tolerance(r1, r2):
                    score = spatial_distance_score(r1, r2)
                    if best_score is None or score < best_score:
                        best_score = score
                        best_j = j

            if best_j is not None:
                used_kg2.add(best_j)
                matched_pairs.append((r1, recs2[best_j]))

    return matched_pairs, overlap_start, overlap_end


# =========================
# Stage 2: 시공간 매칭 결과 -> 전역 엔티티 정렬
# =========================
class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def add(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x):
        self.add(x)
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a, b):
        ra = self.find(a)
        rb = self.find(b)

        if ra == rb:
            return

        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


def choose_component_representative(members):
    """
    대표 엔티티 선택 우선순위
    1) canonical URI가 존재하면 그것 사용
    2) 없으면 로컬명 normalize 기준 가장 안정적인 것
    """
    if len(members) == 1:
        return URIRef(members[0])  # 증거 없으면 원본 유지
    
    canonical_candidates = []
    original_candidates = []

    for uri in members:
        u = URIRef(uri)
        c = canonicalize_entity_name(u)
        if c is not None:
            canonical_candidates.append(str(c))
        original_candidates.append(str(u))

    if canonical_candidates:
        return URIRef(sorted(set(canonical_candidates))[0])

    def sort_key(s):
        return (normalize_entity_name(URIRef(s)) or "", s)

    return URIRef(sorted(set(original_candidates), key=sort_key)[0])


def build_entity_alignment_map_from_matches(position_records, matched_pairs, min_evidence=3):
    """
    시공간 매칭된 position pair를 evidence로 사용해서
    전역 엔티티 정렬 맵 생성
    min_evidence 값 만큼 반복 매칭 되면 동일 엔티티로 간주
    """
    all_entities = set()
    for rec in position_records:
        all_entities.add(str(rec["observed_entity"]))

    pair_counter = Counter()
    pair_time_counter = defaultdict(set)

    for r1, r2 in matched_pairs:
        e1 = str(r1["observed_entity"])
        e2 = str(r2["observed_entity"])

        key = tuple(sorted([e1, e2]))
        pair_counter[key] += 1
        pair_time_counter[key].add(r1["time"])

    uf = UnionFind()
    for ent in all_entities:
        uf.add(ent)

    sorted_pairs = sorted(
        pair_counter.items(),
        key=lambda x: (-x[1], len(pair_time_counter[x[0]]), x[0])
    )

    for (e1, e2), cnt in sorted_pairs:
        if cnt >= min_evidence:
            uf.union(e1, e2)

    components = defaultdict(list)
    for ent in all_entities:
        root = uf.find(ent)
        components[root].append(ent)

    representative_map = {}
    for _, members in components.items():
        rep = choose_component_representative(members)
        for ent in members:
            representative_map[ent] = rep

    return representative_map, pair_counter


# =========================
# Stage 3: 전역 엔티티 정렬 적용 후 위치 관측 병합
# =========================
def build_merged_position_items(all_position_records, representative_map):
    grouped = defaultdict(list)

    for rec in all_position_records:
        orig_ent = str(rec["observed_entity"])
        aligned_ent = representative_map.get(
            orig_ent,
            canonicalize_entity_name(rec["observed_entity"]) or rec["observed_entity"]
        )

        key = (rec["time"], str(aligned_ent))
        grouped[key].append((rec, aligned_ent))

    merged_items = []

    for _, members in grouped.items():
        first_rec = members[0][0]
        aligned_entity = members[0][1]

        original_entities = {m[0]["observed_entity"] for m in members}
        source_files = {m[0]["source_file"] for m in members if m[0]["source_file"] is not None}
        source_rows = {m[0]["source_row"] for m in members if m[0]["source_row"] is not None}
        source_kgs = {m[0]["kg"] for m in members}

        avg_lat = sum(m[0]["lat_val"] for m in members) / len(members)
        avg_long = sum(m[0]["long_val"] for m in members) / len(members)

        merged_predicates = set()
        for m in members:
            merged_predicates.update(m[0].get("predicates", set()))

        merged_items.append({
            "obs_uri": first_rec["obs"],
            "obs_type": "position",
            "observed_entity": aligned_entity,
            "original_entities": original_entities,
            "source_files": source_files,
            "source_rows": source_rows,
            "source_kgs": source_kgs,
            "time": first_rec["time"],
            "time_literal": first_rec["time_literal"],
            "lat_literal": decimal_literal(avg_lat),
            "long_literal": decimal_literal(avg_long),
            "predicates": merged_predicates,
            "member_count": len(members),
        })

    return merged_items


def deduplicate_position_items(items):
    dedup = {}

    for item in items:
        lat_key = round(float(item["lat_literal"]), 10)
        long_key = round(float(item["long_literal"]), 10)
        pred_key = tuple(sorted(str(p) for p in item.get("predicates", set())))

        key = (
            item["time"],
            str(item["observed_entity"]),
            lat_key,
            long_key,
            pred_key,
        )

        if key not in dedup:
            dedup[key] = item
        else:
            dedup[key]["source_files"].update(item["source_files"])
            dedup[key]["source_rows"].update(item["source_rows"])
            dedup[key]["source_kgs"].update(item["source_kgs"])
            dedup[key]["original_entities"].update(item["original_entities"])
            dedup[key]["predicates"].update(item.get("predicates", set()))
            dedup[key]["member_count"] += item["member_count"]

    return list(dedup.values())


def build_position_index(merged_positions):
    idx = {}
    for item in merged_positions:
        key = (item["time"], str(item["observed_entity"]))
        idx[key] = item
    return idx


# =========================
# Stage 4: 전역 엔티티 정렬 적용 후 관계 관측 병합
# - 술어 정규화는 이미 extract 단계에서 완료
# =========================
def relation_is_supported(rec, position_index, representative_map):
    aligned_subject = representative_map.get(
        str(rec["subject_entity"]),
        canonicalize_entity_name(rec["subject_entity"]) or rec["subject_entity"]
    )
    aligned_object = representative_map.get(
        str(rec["object_entity"]),
        canonicalize_entity_name(rec["object_entity"]) or rec["object_entity"]
    )

    t = rec["time"]
    s_ok = (t, str(aligned_subject)) in position_index
    o_ok = (t, str(aligned_object)) in position_index

    return s_ok and o_ok


def build_relation_cluster_key(rec, representative_map):
    aligned_subject = representative_map.get(
        str(rec["subject_entity"]),
        canonicalize_entity_name(rec["subject_entity"]) or rec["subject_entity"]
    )
    aligned_object = representative_map.get(
        str(rec["object_entity"]),
        canonicalize_entity_name(rec["object_entity"]) or rec["object_entity"]
    )

    return (
        rec["time"],
        rec["canonical_relation"],
        str(aligned_subject),
        str(aligned_object),
    )


def merge_relation_records(all_relation_records, position_index, representative_map):
    grouped = defaultdict(list)

    for rec in all_relation_records:
        if not relation_is_supported(rec, position_index, representative_map):
            continue

        key = build_relation_cluster_key(rec, representative_map)
        grouped[key].append(rec)

    merged_items = []

    for _, members in grouped.items():
        first = members[0]

        aligned_subject = representative_map.get(
            str(first["subject_entity"]),
            canonicalize_entity_name(first["subject_entity"]) or first["subject_entity"]
        )
        aligned_object = representative_map.get(
            str(first["object_entity"]),
            canonicalize_entity_name(first["object_entity"]) or first["object_entity"]
        )

        merged_items.append({
            "obs_uri": first["obs"],
            "obs_type": "relation",
            "subject_entity": aligned_subject,
            "object_entity": aligned_object,
            "canonical_relation": first["canonical_relation"],
            "original_subjects": {m["subject_entity"] for m in members},
            "original_objects": {m["object_entity"] for m in members},
            "original_relations": {m["relation_value"] for m in members},
            "source_files": {m["source_file"] for m in members if m["source_file"] is not None},
            "source_rows": {m["source_row"] for m in members if m["source_row"] is not None},
            "source_kgs": {m["kg"] for m in members},
            "time": first["time"],
            "time_literal": first["time_literal"],
            "member_count": len(members),
        })

    return merged_items


def deduplicate_relation_items(items):
    dedup = {}

    for item in items:
        key = (
            item["time"],
            item["canonical_relation"],
            str(item["subject_entity"]),
            str(item["object_entity"]),
        )

        if key not in dedup:
            dedup[key] = item
        else:
            dedup[key]["source_files"].update(item["source_files"])
            dedup[key]["source_rows"].update(item["source_rows"])
            dedup[key]["source_kgs"].update(item["source_kgs"])
            dedup[key]["original_subjects"].update(item["original_subjects"])
            dedup[key]["original_objects"].update(item["original_objects"])
            dedup[key]["original_relations"].update(item["original_relations"])
            dedup[key]["member_count"] += item["member_count"]

    return list(dedup.values())


# =========================
# 최종 그래프 작성
# =========================
def write_merged_graph(merged, position_items, relation_items, out_file):
    merged.bind("stkg", STKG)
    merged.bind("stkgrel", STKGREL)
    merged.bind("geo", GEO)
    merged.bind("owl", OWL)
    merged.bind("xsd", XSD)

    # 위치 관측
    for item in position_items:
        obs_uri = item["obs_uri"]

        merged.add((item["observed_entity"], RDF.type, PLATFORM))

        merged.add((obs_uri, RDF.type, POSITION_OBS))
        merged.add((obs_uri, STKG.observedEntity, item["observed_entity"]))
        merged.add((obs_uri, STKG.time, item["time_literal"]))
        merged.add((obs_uri, GEO.lat, item["lat_literal"]))
        merged.add((obs_uri, GEO.long, item["long_literal"]))

        for pred in item.get("predicates", set()):
            merged.add((obs_uri, HAS_PREDICATE, pred))

        for sf in item["source_files"]:
            merged.add((obs_uri, STKG.sourceFile, sf))

        for sr in item["source_rows"]:
            merged.add((obs_uri, STKG.sourceRow, sr))

        for original_entity in item["original_entities"]:
            if should_emit_same_as(original_entity, item["observed_entity"]):
                merged.add((original_entity, OWL.sameAs, item["observed_entity"]))

    # 관계 관측
    for item in relation_items:
        obs_uri = item["obs_uri"]

        merged.add((item["subject_entity"], RDF.type, PLATFORM))
        merged.add((item["object_entity"], RDF.type, PLATFORM))

        merged.add((obs_uri, RDF.type, RELATION_OBS))
        merged.add((obs_uri, STKG.time, item["time_literal"]))
        merged.add((obs_uri, STKG.subjectEntity, item["subject_entity"]))
        merged.add((obs_uri, STKG.objectEntity, item["object_entity"]))
        merged.add((
            obs_uri,
            RELATION_TYPE_PROP,
            URIRef(f"{str(STKGREL)}{item['canonical_relation']}")
        ))

        for sf in item["source_files"]:
            merged.add((obs_uri, STKG.sourceFile, sf))

        for sr in item["source_rows"]:
            merged.add((obs_uri, STKG.sourceRow, sr))

        for original_subject in item["original_subjects"]:
            if should_emit_same_as(original_subject, item["subject_entity"]):
                merged.add((original_subject, OWL.sameAs, item["subject_entity"]))

        for original_object in item["original_objects"]:
            if should_emit_same_as(original_object, item["object_entity"]):
                merged.add((original_object, OWL.sameAs, item["object_entity"]))

    out_fmt = detect_rdf_format(out_file)
    merged.serialize(out_file, format=out_fmt)


# =========================
# 디버그 / 통계
# =========================
def print_graph_stats(name, graph):
    print(f"\n[{name}] triples: {len(graph)}")
    counter = Counter()
    for _, p, _ in graph:
        counter[str(p)] += 1
    for pred, cnt in counter.most_common():
        print(f"{cnt:5d}  {pred}")


def print_spatiotemporal_match_debug(matched_pairs):
    print("\n[DEBUG] spatiotemporal matched position pairs")
    for r1, r2 in matched_pairs:
        print(
            f"time={r1['time']} | "
            f"{r1['observed_entity']} <-> {r2['observed_entity']} | "
            f"lat_diff={abs(r1['lat_val'] - r2['lat_val'])} | "
            f"long_diff={abs(r1['long_val'] - r2['long_val'])}"
        )


def print_entity_alignment_debug(representative_map, pair_counter):
    print("\n[DEBUG] entity alignment result")
    for orig, rep in sorted(representative_map.items(), key=lambda x: x[0]):
        print(f"{orig} -> {rep}")

    print("\n[DEBUG] entity pair evidence")
    for pair, cnt in pair_counter.most_common():
        print(f"{pair} : {cnt}")


def print_position_alignment_debug(position_items):
    print("\n[DEBUG] merged position items")
    for item in sorted(position_items, key=lambda x: (x["time"], str(x["observed_entity"]))):
        originals = sorted(str(v) for v in item["original_entities"])
        preds = sorted(str(v) for v in item.get("predicates", set()))
        print(
            f"time={item['time']} | rep={item['observed_entity']} | "
            f"members={item['member_count']} | originals={originals} | predicates={preds}"
        )


# =========================
# 메인
# =========================
def merge_graphs(kg1_file: str, kg2_file: str, out_file: str):
    g1 = parse_graph(kg1_file)
    g2 = parse_graph(kg2_file)

    merged = Graph()

    # 1) 관측 추출
    pos_records_kg1 = extract_position_observations(g1, "KG1")
    pos_records_kg2 = extract_position_observations(g2, "KG2")
    rel_records_kg1 = extract_relation_observations(g1, "KG1")
    rel_records_kg2 = extract_relation_observations(g2, "KG2")

    all_position_records = pos_records_kg1 + pos_records_kg2
    all_relation_records = rel_records_kg1 + rel_records_kg2

    # 2) 시공간 기준 위치 매칭
    matched_pairs, overlap_start, overlap_end = build_spatiotemporal_position_matches(
        pos_records_kg1,
        pos_records_kg2
    )

    export_matched_position_pairs_to_csv(matched_pairs, MATCHED_POSITION_PAIR_CSV)

    # 3) 시공간 매칭 결과를 바탕으로 전역 엔티티 정렬
    representative_map, pair_counter = build_entity_alignment_map_from_matches(
        all_position_records,
        matched_pairs,
        min_evidence=1
    )

    # 4) 엔티티 정렬 결과를 적용해서 위치 관측 병합
    merged_positions = build_merged_position_items(
        all_position_records,
        representative_map
    )
    merged_positions = deduplicate_position_items(merged_positions)

    position_index = build_position_index(merged_positions)

    # 5) 같은 엔티티 정렬 결과를 적용해서 관계 관측 병합
    merged_relations = merge_relation_records(
        all_relation_records,
        position_index,
        representative_map
    )
    merged_relations = deduplicate_relation_items(merged_relations)

    # 6) 최종 그래프 작성
    write_merged_graph(merged, merged_positions, merged_relations, out_file)

    print(f"[DONE] merged graph saved to: {out_file}")
    print(f"[INFO] matched position pair csv: {MATCHED_POSITION_PAIR_CSV}")
    print(f"[INFO] input position observations KG1: {len(pos_records_kg1)}")
    print(f"[INFO] input position observations KG2: {len(pos_records_kg2)}")
    print(f"[INFO] spatiotemporal matched position pairs: {len(matched_pairs)}")
    print(f"[INFO] merged position observations: {len(merged_positions)}")
    print(f"[INFO] input relation observations KG1: {len(rel_records_kg1)}")
    print(f"[INFO] input relation observations KG2: {len(rel_records_kg2)}")
    print(f"[INFO] merged relation observations: {len(merged_relations)}")
    print(f"[INFO] overlap time range: {overlap_start} ~ {overlap_end}")
    print(f"[INFO] LAT_TOL  = {LAT_TOL}")
    print(f"[INFO] LONG_TOL = {LONG_TOL}")

    print_graph_stats("KG1", g1)
    print_graph_stats("KG2", g2)
    print_graph_stats("MERGED", merged)

    # 필요시 디버그 활성화
    # print_spatiotemporal_match_debug(matched_pairs)
    # print_entity_alignment_debug(representative_map, pair_counter)
    # print_position_alignment_debug(merged_positions)


if __name__ == "__main__":
    merge_graphs(KG1_FILE, KG2_FILE, OUT_FILE)