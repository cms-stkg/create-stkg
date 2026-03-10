## 사용법

1. conda 가상환경 생성 및 실행 
    
    conda create -n yago-stkg python=3.11
    
    conda activate yago-stkg

2. 의존성 설치
    
    pip install -r requirements.txt

3. /input-data/stkg/ 경로에 stkg로 생성할 로우 데이터(csv) 파일 삽입
4. KG 생성 명령어
    
    python 03c-make-stkg.py --in (로우 데이터) --out (출력 폴더) --graph_mode by_file --file_tag KG3 --emit nq

    - emit : nq, trig, ttl 중 선택 가능
5. KG 병합 명령어

    python merge-kg.py --kg1 C:\Users\foxes\LYS\cybermarine\STKG\yago-4.5\yago-data\KG1.nq --kg2 C:\Users\foxes\LYS\cybermarine\STKG\yago-4.5\yago-data\KG2.nq --out yago-data\KG1_KG2_merged_dedup_2 --emit nq

## KG 경로
    1. 시나리오 별 KG
        - yago-data\KG1.nq
        - yago-data\KG2.nq
        - yago-data\KG3.nq
    2. KG1, KG2 병합 KG
        - yago-data\KG1_KG2_merged_dedup.nq