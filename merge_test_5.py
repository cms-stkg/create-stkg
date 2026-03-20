#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
from collections import defaultdict
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD, OWL
from collections import Counter

# =========================
# 설정
# =========================
KG1_FILE = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data6\\KG1\\stkg-final.ttl"
KG2_FILE = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data6\\KG_e2\\stkg-final.ttl"
OUT_FILE = "C:\\Users\\foxes\\LYS\\cybermarine\\STKG\\create-stkg\\yago-data6\\output\\merge.ttl"

# 센서 오차 허용 범위
SENSOR_ERROR_TOLERANCE = 1
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
# 실제 RDF 구조에 맞게 필요시 수정
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
# - 엔티티 정렬 기준이 아니라
#   병합 후 대표 URI 선택용
# =========================
CANONICAL_ENTITY_SETS = {
    "entity_t72": {
        "T-72", "T72"
    },
    "entity_t62": {
        "T-62", "T62"
    },
    "entity_btr80": {
        "BTR-80", "BTR80"
    },
    "entity_k2blackpanther": {
        "K2 Black Panther", "K2BlackPanther", "K2_Black_Panther"
    },
    "entity_t90": {
        "T-90", "T90"
    },
}

CANONICAL_PREDICATE_SETS = {
    "inFrontOf": {
        "in_front_of", "ahead_of", "located_ahead_of"
    },
    "behind": {
        "in_back_of", "behind", "behind_of", "located_behind_of"
    },
    "sideBySide": {
        "side_by_side", "beside", "next_to"
    },
}


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
        raise ValueError(
            f"Unsupported RDF file extension: {ext}. Only .ttl and .nt are supported."
        )


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


def normalize_entity_name(entity_uri: URIRef):
    if entity_uri is None:
        return None
    s = str(entity_uri)
    local = s.rstrip("/").split("/")[-1]
    return normalize_text(local)


def canonicalize_entity_name(entity_uri: URIRef):
    """
    canonical_set 기반 대표 엔티티 URI 생성용.
    정렬 기준으로 사용하지 않고, 병합 후 대표 URI 선택용으로만 사용.
    """
    norm = normalize_entity_name(entity_uri)
    if norm is None:
        return None

    for canonical_id, alias_set in CANONICAL_ENTITY_SETS.items():
        normalized_aliases = {normalize_text(a) for a in alias_set}
        if norm in normalized_aliases:
            return URIRef(f"http://example.org/stkg/id/{canonical_id}")

    return None


def canonicalize_predicate(raw_value):
    norm = normalize_text(raw_value)
    if norm is None:
        return None

    for canonical_pred, alias_set in CANONICAL_PREDICATE_SETS.items():
        normalized_aliases = {normalize_text(a) for a in alias_set}
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


def within_sensor_tolerance(rec1, rec2):
    if rec1["time"] != rec2["time"]:
        return False

    lat_ok = abs(rec1["lat_val"] - rec2["lat_val"]) <= LAT_TOL
    long_ok = abs(rec1["long_val"] - rec2["long_val"]) <= LONG_TOL
    return lat_ok and long_ok


# =========================
# 시간 범위 계산
# A, A' = 부분 overlap 처리
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
# Gate 1: 위치관측 시공간 정렬
# overlap 시간대만 cross-KG 병합
# =========================
def find_matching_position_cluster(rec, clusters):
    for idx, cluster in enumerate(clusters):
        rep = cluster["members"][0]
        if within_sensor_tolerance(rec, rep):
            return idx
    return None


def build_position_clusters(records):
    time_groups = defaultdict(list)

    for rec in records:
        time_groups[rec["time"]].append(rec)

    clusters = []
    for _, same_time_records in time_groups.items():
        local_clusters = []

        for rec in same_time_records:
            matched_idx = find_matching_position_cluster(rec, local_clusters)

            if matched_idx is None:
                local_clusters.append({"members": [rec]})
            else:
                local_clusters[matched_idx]["members"].append(rec)

        clusters.extend(local_clusters)

    return clusters


def choose_representative_entity(members):
    """
    엔티티 정렬은 이미 시공간 cluster 결과로 끝났고,
    여기서는 cluster의 대표 URI만 선택한다.
    """
    canonical_candidates = []

    for m in members:
        c = canonicalize_entity_name(m["observed_entity"])
        if c is not None:
            canonical_candidates.append(c)

    if canonical_candidates:
        return canonical_candidates[0]

    return members[0]["observed_entity"]


def build_merged_position_item(cluster):
    members = cluster["members"]
    first = members[0]

    original_entities = {m["observed_entity"] for m in members}
    source_files = {m["source_file"] for m in members if m["source_file"] is not None}
    source_rows = {m["source_row"] for m in members if m["source_row"] is not None}
    source_kgs = {m["kg"] for m in members}

    avg_lat = sum(m["lat_val"] for m in members) / len(members)
    avg_long = sum(m["long_val"] for m in members) / len(members)

    final_entity = choose_representative_entity(members)

    return {
        "obs_uri": first["obs"],
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

    overlap_records = []
    passthrough_records = []

    for rec in kg1_records + kg2_records:
        if is_in_overlap(rec["time"], overlap_start, overlap_end):
            overlap_records.append(rec)
        else:
            passthrough_records.append(rec)

    # overlap 구간은 시공간 기준으로만 cluster
    clusters = build_position_clusters(overlap_records)
    merged_items = [build_merged_position_item(c) for c in clusters]

    # non-overlap 구간은 그대로 유지
    for rec in passthrough_records:
        final_entity = choose_representative_entity([rec])

        merged_items.append({
            "obs_uri": rec["obs"],
            "obs_type": "position",
            "observed_entity": final_entity,
            "original_entities": {rec["observed_entity"]},
            "source_files": {rec["source_file"]} if rec["source_file"] is not None else set(),
            "source_rows": {rec["source_row"]} if rec["source_row"] is not None else set(),
            "source_kgs": {rec["kg"]},
            "time": rec["time"],
            "time_literal": rec["time_literal"],
            "lat_literal": Literal(rec["lat_val"], datatype=XSD.double),
            "long_literal": Literal(rec["long_val"], datatype=XSD.double),
            "member_count": 1,
        })

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
    위치 병합 결과를 기반으로
    원본 엔티티 URI -> 대표 엔티티 URI 매핑 생성
    """
    entity_map = {}

    for item in position_items:
        rep = item["observed_entity"]
        for orig in item["original_entities"]:
            entity_map[str(orig)] = rep

    return entity_map


# =========================
# Gate 1~2: 관계관측 정렬
# 술어 정렬 -> 시간 정렬 -> 위치 병합 결과 기반 엔티티 정렬
# =========================
def relation_is_supported(rec, position_index, entity_map):
    aligned_subject = entity_map.get(str(rec["subject_entity"]))
    aligned_object = entity_map.get(str(rec["object_entity"]))

    if aligned_subject is None or aligned_object is None:
        return False

    t = rec["time"]
    s_ok = (t, str(aligned_subject)) in position_index
    o_ok = (t, str(aligned_object)) in position_index

    return s_ok and o_ok


def build_relation_cluster_key(rec, entity_map):
    aligned_subject = entity_map.get(str(rec["subject_entity"]))
    aligned_object = entity_map.get(str(rec["object_entity"]))

    if aligned_subject is None or aligned_object is None:
        return None

    return (
        rec["time"],
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

    # overlap 구간은 병합
    for rec in overlap_records:
        key = build_relation_cluster_key(rec, entity_map)
        if key is None:
            continue
        grouped[key].append(rec)

    merged_items = []

    for _, members in grouped.items():
        first = members[0]
        aligned_subject = entity_map.get(str(first["subject_entity"]))
        aligned_object = entity_map.get(str(first["object_entity"]))

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

    # non-overlap 구간은 개별 유지
    for rec in passthrough_records:
        aligned_subject = entity_map.get(str(rec["subject_entity"]))
        aligned_object = entity_map.get(str(rec["object_entity"]))

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
            "time": rec["time"],
            "time_literal": rec["time_literal"],
            "member_count": 1,
        })

    return merged_items


# =========================
# Gate 3: 충돌 해결 / dedup
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
            if str(original_entity) != str(item["observed_entity"]):
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
            if str(original_subject) != str(item["subject_entity"]):
                merged.add((original_subject, OWL.sameAs, item["subject_entity"]))

        for original_object in item["original_objects"]:
            if str(original_object) != str(item["object_entity"]):
                merged.add((original_object, OWL.sameAs, item["object_entity"]))

    out_fmt = detect_rdf_format(out_file)
    merged.serialize(out_file, format=out_fmt)

def print_graph_stats(name, graph):
    print(f"\n[{name}] triples: {len(graph)}")
    counter = Counter()
    for s, p, o in graph:
        counter[str(p)] += 1
    for pred, cnt in counter.most_common():
        print(f"{cnt:5d}  {pred}")

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
    
    # merge_graphs() 마지막 직전
    print_graph_stats("KG1", g1)
    print_graph_stats("KG2", g2)
    print_graph_stats("MERGED", merged)
        
if __name__ == "__main__":
    merge_graphs(KG1_FILE, KG2_FILE, OUT_FILE)