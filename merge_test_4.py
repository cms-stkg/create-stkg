#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from rdflib import Graph, Namespace, URIRef
from rdflib import Literal
from rdflib.namespace import RDF, XSD

# =========================
# 설정
# =========================
KG1_FILE = "/home/workspace/merge_KG/create-stkg/input-data/raw_data_ver2.2_KG/stkg-final_KG1_ver2.2.ttl"
KG2_FILE = "/home/workspace/merge_KG/create-stkg/input-data/raw_data_ver2.2_KG/stkg-final_KG2_e2_ver2.2.ttl"
OUT_FILE = "/home/workspace/merge_KG/create-stkg/output/merged_dedup_ver2.2_integrated_1.ttl"

# 센서 오차 허용 범위
# idx 0: 1m, idx 1: 2m
SENSOR_ERROR_TOLERANCE = 1
sensorErrorTol = [2, 1] 

# 센서 오차 허용 범위, 위도 35, 경도 130 기준
LAT_TOL = (5.41e-5 / 3) / sensorErrorTol[SENSOR_ERROR_TOLERANCE] # (약 1m)
LONG_TOL = (8.78e-5 / 4) / sensorErrorTol[SENSOR_ERROR_TOLERANCE] # (약 1m)

# Namespace
STKG = Namespace("http://example.org/stkg/")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
OWL = Namespace("http://www.w3.org/2002/07/owl#")

POSITION_OBS = URIRef("http://example.org/stkg/id/PositionObservation")


# =========================
# 엔티티 정규화
# =========================
def normalize_entity_name(entity_uri: URIRef):
    """
    예:
      http://example.org/stkg/id/platform_BTR-80
      -> platformbtr80

      http://example.org/stkg/id/platform_BTR80
      -> platformbtr80
    """
    if entity_uri is None:
        return None

    s = str(entity_uri)
    local = s.rstrip("/").split("/")[-1]
    local = local.lower()
    local = re.sub(r"[-_\s]", "", local)
    return local


def make_canonical_entity_uri(entity_uri: URIRef):
    """
    표기만 다른 동일 객체를 하나의 canonical URI로 통일할 때 사용
    """
    norm = normalize_entity_name(entity_uri)
    if norm is None:
        return None
    return URIRef(f"http://example.org/stkg/id/{norm}")


# =========================
# observation 추출
# =========================
def extract_position_observations(graph: Graph):
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

        try:
            lat_val = float(lat)
            lon_val = float(lon)
        except Exception:
            continue

        records.append({
            "obs": obs,
            "observed_entity": observed_entity,
            "entity_norm": normalize_entity_name(observed_entity),
            "source_file": source_file,
            "source_row": source_row,
            "time": str(time),
            "time_literal": time,
            "lat": lat,
            "long": lon,
            "lat_val": lat_val,
            "long_val": lon_val,
        })

    return records


# =========================
# 비교 함수
# =========================
def within_sensor_tolerance(rec1, rec2):
    """
    같은 시간이고, lat/long 모두 허용 오차 이내인지 확인
    """
    if rec1["time"] != rec2["time"]:
        return False

    lat_ok = abs(rec1["lat_val"] - rec2["lat_val"]) <= LAT_TOL
    long_ok = abs(rec1["long_val"] - rec2["long_val"]) <= LONG_TOL

    return lat_ok and long_ok


def entity_match(rec1, rec2):
    """
    엔티티가 같은 객체인지 판단
    - URI 완전 동일 OR
    - 정규화 결과 동일
    """
    uri_same = str(rec1["observed_entity"]) == str(rec2["observed_entity"])
    norm_same = rec1["entity_norm"] == rec2["entity_norm"]

    return uri_same or norm_same


# =========================
# 클러스터 병합
# =========================
def find_matching_cluster(rec, clusters):
    """
    rec가 들어갈 수 있는 기존 cluster 탐색
    조건:
    - cluster 대표 observation과 time 동일
    - lat/long 허용오차 이내
    - entity 동일 객체로 판단 가능
    """
    for idx, cluster in enumerate(clusters):
        rep = cluster["members"][0]

        if within_sensor_tolerance(rec, rep) and entity_match(rec, rep):
            return idx

    return None


def build_merged_item(cluster):
    members = cluster["members"]
    first = members[0]

    original_entities = {m["observed_entity"] for m in members}
    entity_uri_strings = {str(e) for e in original_entities}

    source_files = {m["source_file"] for m in members if m["source_file"] is not None}
    source_rows = {m["source_row"] for m in members if m["source_row"] is not None}

    # 엔티티 URI가 모두 완전히 같으면 기존 URI 유지
    if len(entity_uri_strings) == 1:
        final_entity = next(iter(original_entities))
        add_sameas = False
    else:
        final_entity = make_canonical_entity_uri(first["observed_entity"])
        add_sameas = True

    # 좌표 평균
    avg_lat = sum(m["lat_val"] for m in members) / len(members)
    avg_long = sum(m["long_val"] for m in members) / len(members)

    return {
        "obs_uri": first["obs"],
        "observed_entity": final_entity,
        "original_entities": original_entities,
        "add_sameas": add_sameas,
        "source_files": source_files,
        "source_rows": source_rows,
        "time_literal": first["time_literal"],
        "lat_literal": Literal(avg_lat, datatype=XSD.double),
        "long_literal": Literal(avg_long, datatype=XSD.double),
        "member_count": len(members),
    }


# =========================
# 병합 메인
# =========================
def merge_graphs(kg1_file: str, kg2_file: str, out_file: str):
    g1 = Graph()
    g2 = Graph()

    g1.parse(kg1_file, format="turtle")
    g2.parse(kg2_file, format="turtle")

    merged = Graph()
    merged.bind("stkg", STKG)
    merged.bind("geo1", GEO)
    merged.bind("xsd", XSD)
    merged.bind("owl", OWL)

    all_obs = extract_position_observations(g1) + extract_position_observations(g2)

    # 시간별로 먼저 나누면 비교량이 줄어듦
    time_groups = {}
    for rec in all_obs:
        time_groups.setdefault(rec["time"], []).append(rec)

    final_records = []

    for _, records_at_same_time in time_groups.items():
        clusters = []

        for rec in records_at_same_time:
            matched_idx = find_matching_cluster(rec, clusters)

            if matched_idx is None:
                clusters.append({"members": [rec]})
            else:
                clusters[matched_idx]["members"].append(rec)

        for cluster in clusters:
            merged_item = build_merged_item(cluster)
            final_records.append(merged_item)

    # =========================
    # 최종 그래프 작성
    # =========================
    for item in final_records:
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

        # URI가 달라 canonical로 통일된 경우만 sameAs 추가
        if item["add_sameas"]:
            for original_entity in item["original_entities"]:
                if str(original_entity) != str(item["observed_entity"]):
                    merged.add((original_entity, OWL.sameAs, item["observed_entity"]))

    merged.serialize(out_file, format="turtle")

    print(f"[DONE] merged graph saved to: {out_file}")
    print(f"[INFO] input observations: {len(all_obs)}")
    print(f"[INFO] merged observations: {len(final_records)}")
    print(f"[INFO] LAT_TOL  = {LAT_TOL}")
    print(f"[INFO] LONG_TOL = {LONG_TOL}")


if __name__ == "__main__":
    merge_graphs(KG1_FILE, KG2_FILE, OUT_FILE)