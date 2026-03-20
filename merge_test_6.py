#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
from collections import defaultdict, Counter
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD, OWL

# =========================
# 설정
# =========================
KG1_FILE = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data6\\KG1\\stkg-final.ttl"
KG2_FILE = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data6\\KG_e2\\stkg-final.ttl"
OUT_FILE = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data6\\output\\merge.ttl"

# 센서 오차 허용 범위
# 0 -> 더 엄격(약 0.5m 수준), 1 -> 약 1m 수준
SENSOR_ERROR_TOLERANCE = 0
sensorErrorTol = [2, 1]

LAT_TOL = (5.41e-5 / 6) / sensorErrorTol[SENSOR_ERROR_TOLERANCE]
LONG_TOL = (8.78e-5 / 8) / sensorErrorTol[SENSOR_ERROR_TOLERANCE]

# =========================
# Namespace
# =========================
STKG = Namespace("http://example.org/stkg/")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

POSITION_OBS = URIRef("http://example.org/stkg/id/PositionObservation")
RELATION_OBS = URIRef("http://example.org/stkg/id/SpatialRelationObservation")

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
# - alias 정렬 및 대표 URI 선택용
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

# 미리 alias 역색인 생성
ENTITY_ALIAS_TO_CANONICAL = {}
for canonical_id, alias_set in CANONICAL_ENTITY_SETS.items():
    for alias in alias_set:
        norm_alias = re.sub(r"[-_\s]", "", alias.strip().lower())
        ENTITY_ALIAS_TO_CANONICAL[norm_alias] = canonical_id


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

def normalize_relation_surface(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    s = re.sub(r"\s+", "_", s)   # 공백 -> _
    s = s.replace("-", "_")      # - -> _
    return s

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


def get_local_name(uri_or_value):
    if uri_or_value is None:
        return None
    s = str(uri_or_value)
    return s.rstrip("/").split("/")[-1]


def normalize_entity_name(entity_uri: URIRef):
    if entity_uri is None:
        return None
    return normalize_text(get_local_name(entity_uri))


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
    norm = normalize_relation_surface(raw_value)
    if norm is None:
        return None

    for canonical_pred, alias_set in CANONICAL_PREDICATE_SETS.items():
        normalized_aliases = {normalize_relation_surface(a) for a in alias_set}
        if norm in normalized_aliases:
            return canonical_pred

    return norm


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


def entity_matchable(rec1, rec2):
    """
    두 엔티티가 병합 후보가 될 수 있는지 판단.
    - canonical alias가 같으면 허용
    - canonical이 없으면 normalize 이름이 완전히 같을 때만 허용
    """
    c1 = get_entity_canonical_id(rec1["observed_entity"])
    c2 = get_entity_canonical_id(rec2["observed_entity"])

    if c1 is not None and c2 is not None:
        return c1 == c2

    n1 = normalize_entity_name(rec1["observed_entity"])
    n2 = normalize_entity_name(rec2["observed_entity"])
    return n1 == n2


def within_sensor_tolerance(rec1, rec2):
    if rec1["time"] != rec2["time"]:
        return False

    # 엔티티 이름/alias가 일치하는 경우에만 병합 허용
    if not entity_matchable(rec1, rec2):
        return False

    lat_ok = abs(rec1["lat_val"] - rec2["lat_val"]) <= LAT_TOL
    long_ok = abs(rec1["long_val"] - rec2["long_val"]) <= LONG_TOL
    return lat_ok and long_ok


def should_emit_same_as(original_entity, representative_entity):
    """
    owl:sameAs는 alias 정렬일 때만 허용.
    다른 플랫폼(T-72 vs BTR-80)은 절대 sameAs를 만들지 않음.
    """
    if str(original_entity) == str(representative_entity):
        return False

    orig_c = get_entity_canonical_id(original_entity)
    rep_c = get_entity_canonical_id(representative_entity)

    if orig_c is not None and rep_c is not None:
        return orig_c == rep_c

    orig_n = normalize_entity_name(original_entity)
    rep_n = normalize_entity_name(representative_entity)
    return orig_n == rep_n


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
        })

    return records


def extract_relation_observations(graph: Graph, kg_label: str):
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

        records.append({
            "kg": kg_label,
            "obs": obs,
            "obs_type": "relation",
            "subject_entity": ent1,
            "object_entity": ent2,
            "relation_value": rel_val,
            "canonical_relation": canonicalize_predicate(rel_val),
            "source_file": source_file,
            "source_row": source_row,
            "time": safe_time_key(time),
            "time_literal": time,
        })

    return records


# =========================
# Gate 1: 위치관측 병합
# - cross-KG끼리만 병합
# - 같은 엔티티(alias 포함) + 같은 시간 + 허용 오차 이내
# =========================
def choose_representative_entity(members):
    canonical_candidates = []

    for m in members:
        c = canonicalize_entity_name(m["observed_entity"])
        if c is not None:
            canonical_candidates.append(c)

    if canonical_candidates:
        return canonical_candidates[0]

    return members[0]["observed_entity"]


def build_merged_position_item(members):
    first = members[0]

    original_entities = {m["observed_entity"] for m in members}
    source_files = {m["source_file"] for m in members if m["source_file"] is not None}
    source_rows = {m["source_row"] for m in members if m["source_row"] is not None}
    source_kgs = {m["kg"] for m in members}

    avg_lat = sum(m["lat_val"] for m in members) / len(members)
    avg_long = sum(m["long_val"] for m in members) / len(members)

    final_entity = choose_representative_entity(members)

    return {
        "obs_uri": first["obs"],   # 대표 observation URI는 첫 번째 것 유지
        "obs_type": "position",
        "observed_entity": final_entity,
        "original_entities": original_entities,
        "source_files": source_files,
        "source_rows": source_rows,
        "source_kgs": source_kgs,
        "time": first["time"],
        "time_literal": first["time_literal"],
        "lat_literal": Literal(avg_lat, datatype=XSD.double),
        "long_literal": Literal(avg_long, datatype=XSD.double),
        "member_count": len(members),
    }


def merge_position_records_with_overlap(kg1_records, kg2_records):
    overlap_start, overlap_end = compute_overlap_range(kg1_records, kg2_records)

    # 시간별 분리
    kg1_by_time = defaultdict(list)
    kg2_by_time = defaultdict(list)

    passthrough_items = []

    for rec in kg1_records:
        if is_in_overlap(rec["time"], overlap_start, overlap_end):
            kg1_by_time[rec["time"]].append(rec)
        else:
            passthrough_items.append(build_merged_position_item([rec]))

    for rec in kg2_records:
        if is_in_overlap(rec["time"], overlap_start, overlap_end):
            kg2_by_time[rec["time"]].append(rec)
        else:
            passthrough_items.append(build_merged_position_item([rec]))

    merged_items = []
    matched_kg1 = set()
    matched_kg2 = set()

    all_times = sorted(set(kg1_by_time.keys()) | set(kg2_by_time.keys()))

    for t in all_times:
        recs1 = kg1_by_time.get(t, [])
        recs2 = kg2_by_time.get(t, [])

        # cross-KG pairwise matching만 수행
        for i, r1 in enumerate(recs1):
            best_j = None
            best_score = None

            for j, r2 in enumerate(recs2):
                if (t, j) in matched_kg2:
                    continue

                if within_sensor_tolerance(r1, r2):
                    score = (
                        abs(r1["lat_val"] - r2["lat_val"]) +
                        abs(r1["long_val"] - r2["long_val"])
                    )
                    if best_score is None or score < best_score:
                        best_score = score
                        best_j = j

            if best_j is not None:
                matched_kg1.add((t, i))
                matched_kg2.add((t, best_j))
                merged_items.append(build_merged_position_item([r1, recs2[best_j]]))

        # unmatched KG1
        for i, r1 in enumerate(recs1):
            if (t, i) not in matched_kg1:
                merged_items.append(build_merged_position_item([r1]))

        # unmatched KG2
        for j, r2 in enumerate(recs2):
            if (t, j) not in matched_kg2:
                merged_items.append(build_merged_position_item([r2]))

    merged_items.extend(passthrough_items)
    return merged_items, overlap_start, overlap_end


# =========================
# 위치 병합 결과 인덱스 / 엔티티 맵
# =========================
def build_position_index(merged_positions):
    idx = {}
    for item in merged_positions:
        key = (item["time"], str(item["observed_entity"]))
        idx[key] = item
    return idx


def build_entity_alignment_map(position_items):
    """
    시간 포함 매핑으로 수정
    (time, original_entity_uri) -> representative_entity_uri
    """
    entity_map = {}

    for item in position_items:
        rep = item["observed_entity"]
        t = item["time"]
        for orig in item["original_entities"]:
            entity_map[(t, str(orig))] = rep

    return entity_map


# =========================
# Gate 2: 관계관측 병합
# - 위치 병합 결과의 시간 포함 entity_map 사용
# =========================
def relation_is_supported(rec, position_index, entity_map):
    t = rec["time"]
    aligned_subject = entity_map.get((t, str(rec["subject_entity"])))
    aligned_object = entity_map.get((t, str(rec["object_entity"])))

    if aligned_subject is None or aligned_object is None:
        return False

    s_ok = (t, str(aligned_subject)) in position_index
    o_ok = (t, str(aligned_object)) in position_index

    return s_ok and o_ok


def build_relation_cluster_key(rec, entity_map):
    t = rec["time"]
    aligned_subject = entity_map.get((t, str(rec["subject_entity"])))
    aligned_object = entity_map.get((t, str(rec["object_entity"])))

    if aligned_subject is None or aligned_object is None:
        return None

    return (
        t,
        rec["canonical_relation"],
        str(aligned_subject),
        str(aligned_object),
    )


def merge_relation_records_with_overlap(
    kg1_records,
    kg2_records,
    position_index,
    entity_map,
    overlap_start,
    overlap_end
):
    overlap_records = []
    passthrough_records = []

    for rec in kg1_records + kg2_records:
        if not relation_is_supported(rec, position_index, entity_map):
            continue

        if is_in_overlap(rec["time"], overlap_start, overlap_end):
            overlap_records.append(rec)
        else:
            passthrough_records.append(rec)

    grouped = defaultdict(list)

    for rec in overlap_records:
        key = build_relation_cluster_key(rec, entity_map)
        if key is None:
            continue
        grouped[key].append(rec)

    merged_items = []

    for _, members in grouped.items():
        first = members[0]
        t = first["time"]
        aligned_subject = entity_map.get((t, str(first["subject_entity"])))
        aligned_object = entity_map.get((t, str(first["object_entity"])))

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
            "time": t,
            "time_literal": first["time_literal"],
            "member_count": len(members),
        })

    for rec in passthrough_records:
        t = rec["time"]
        aligned_subject = entity_map.get((t, str(rec["subject_entity"])))
        aligned_object = entity_map.get((t, str(rec["object_entity"])))

        if aligned_subject is None or aligned_object is None:
            continue

        merged_items.append({
            "obs_uri": rec["obs"],
            "obs_type": "relation",
            "subject_entity": aligned_subject,
            "object_entity": aligned_object,
            "canonical_relation": rec["canonical_relation"],
            "original_subjects": {rec["subject_entity"]},
            "original_objects": {rec["object_entity"]},
            "original_relations": {rec["relation_value"]},
            "source_files": {rec["source_file"]} if rec["source_file"] is not None else set(),
            "source_rows": {rec["source_row"]} if rec["source_row"] is not None else set(),
            "source_kgs": {rec["kg"]},
            "time": t,
            "time_literal": rec["time_literal"],
            "member_count": 1,
        })

    return merged_items


# =========================
# Gate 3: dedup
# =========================
def deduplicate_position_items(items):
    dedup = {}

    for item in items:
        key = (
            item["time"],
            str(item["observed_entity"]),
            str(item["lat_literal"]),
            str(item["long_literal"]),
        )

        if key not in dedup:
            dedup[key] = item
        else:
            dedup[key]["source_files"].update(item["source_files"])
            dedup[key]["source_rows"].update(item["source_rows"])
            dedup[key]["source_kgs"].update(item["source_kgs"])
            dedup[key]["original_entities"].update(item["original_entities"])
            dedup[key]["member_count"] += item["member_count"]

    return list(dedup.values())


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
    merged.bind("geo", GEO)
    merged.bind("owl", OWL)
    merged.bind("xsd", XSD)

    # 위치 관측
    for item in position_items:
        obs_uri = item["obs_uri"]

        merged.add((obs_uri, RDF.type, POSITION_OBS))
        merged.add((obs_uri, STKG.observedEntity, item["observed_entity"]))
        merged.add((obs_uri, STKG.time, item["time_literal"]))
        merged.add((obs_uri, GEO.lat, item["lat_literal"]))
        merged.add((obs_uri, GEO.long, item["long_literal"]))

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

        merged.add((obs_uri, RDF.type, RELATION_OBS))
        merged.add((obs_uri, STKG.time, item["time_literal"]))
        merged.add((obs_uri, STKG.subjectEntity, item["subject_entity"]))
        merged.add((obs_uri, STKG.objectEntity, item["object_entity"]))
        merged.add((obs_uri, STKG.spatialRelation, Literal(item["canonical_relation"])))

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


def print_graph_stats(name, graph):
    print(f"\n[{name}] triples: {len(graph)}")
    counter = Counter()
    for _, p, _ in graph:
        counter[str(p)] += 1
    for pred, cnt in counter.most_common():
        print(f"{cnt:5d}  {pred}")


def print_position_alignment_debug(position_items):
    print("\n[DEBUG] merged position items")
    for item in sorted(position_items, key=lambda x: (x["time"], str(x["observed_entity"]))):
        originals = sorted(str(v) for v in item["original_entities"])
        print(
            f"time={item['time']} | rep={item['observed_entity']} | "
            f"members={item['member_count']} | originals={originals}"
        )


# =========================
# 메인
# =========================
def merge_graphs(kg1_file: str, kg2_file: str, out_file: str):
    g1 = parse_graph(kg1_file)
    g2 = parse_graph(kg2_file)

    merged = Graph()

    # 위치 관측 추출
    pos_records_kg1 = extract_position_observations(g1, "KG1")
    pos_records_kg2 = extract_position_observations(g2, "KG2")

    # 관계 관측 추출
    rel_records_kg1 = extract_relation_observations(g1, "KG1")
    rel_records_kg2 = extract_relation_observations(g2, "KG2")

    # 위치 관측 병합
    merged_positions, overlap_start, overlap_end = merge_position_records_with_overlap(
        pos_records_kg1,
        pos_records_kg2
    )
    merged_positions = deduplicate_position_items(merged_positions)

    # 위치 병합 결과 기반 인덱스 / 엔티티 정렬 맵
    position_index = build_position_index(merged_positions)
    entity_map = build_entity_alignment_map(merged_positions)

    # 관계 관측 병합
    merged_relations = merge_relation_records_with_overlap(
        rel_records_kg1,
        rel_records_kg2,
        position_index,
        entity_map,
        overlap_start,
        overlap_end
    )
    merged_relations = deduplicate_relation_items(merged_relations)

    # 최종 그래프 작성
    write_merged_graph(merged, merged_positions, merged_relations, out_file)

    print(f"[DONE] merged graph saved to: {out_file}")
    print(f"[INFO] input position observations KG1: {len(pos_records_kg1)}")
    print(f"[INFO] input position observations KG2: {len(pos_records_kg2)}")
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

    # 디버깅용
    # print_position_alignment_debug(merged_positions)


if __name__ == "__main__":
    merge_graphs(KG1_FILE, KG2_FILE, OUT_FILE)