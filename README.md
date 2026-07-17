# Logstash Worker 스레드 벤치마크 보고서

> 방화벽 로그 Aggregate 파이프라인의 `pipeline.workers` 변화에 따른 성능·정합성 분석

---

## 1. 개요

### 1.1 목적

본 문서는 대용량 방화벽 로그 수집 환경에서 **Logstash Worker 스레드 개수(`pipeline.workers`)가 처리 성능, 시스템 리소스 사용량, 그리고 통계 병합(Aggregate) 결과의 정합성에 미치는 영향**을 분석한 벤치마크 보고서이다.

제공된 10분치 방화벽 로그(약 100만 건, 550MB)를 처리하는 Standalone ELK 파이프라인을 구축하고, Worker 개수를 `1 → 4 → 8`로 변화시키며 측정하였다.

### 1.2 핵심 결론 요약

| # | 결론 |
|:---:|---|
| **1** | Worker 1 → 4에서 유입 처리 시간이 **210초 → 70초(3.00배)** 로 단축되었으나, 4 → 8에서는 **70초 → 60초(3.50배)** 로 개선폭이 급감했다. |
| **2** | **End-to-End 시간은 W4·W8 모두 90초로 동일**하다. Worker 8이 유입에서 번 10초를 `aggregate` 20초 타임아웃 드레인 구간이 그대로 반납했기 때문이다. |
| **3** | **어떤 하드웨어 자원도 포화되지 않았다.** CPU 44.5%, Disk 0.5% 미만, JVM Heap 64%. 병목은 `aggregate` 필터의 **맵 접근 직렬화**와 **드레인 고정 오버헤드**다. |
| **4** | **워커당 CPU 활용률이 94.4% → 63.8% → 44.5%로 붕괴**했다. Worker를 8배 늘렸으나 CPU 사용량은 3.8배만 증가했다. |
| **5** | Aggregate 병합으로 이벤트 건수를 **44.7~46.4% 축소**했으며, `sum` 집계는 전 케이스에서 보존되었다. |
| **6** | Worker 증가 시 **20초 윈도우의 이벤트 밀도 분포가 왜곡**되었다. 총 윈도우 수가 552,904 → 550,638 → 535,752건으로 감소했다. |
| **7** | **`aggregate` 필터 사용 시 `pipeline.workers=1`은 선택이 아닌 필수**다. 처리량 확보는 워커 증설이 아니라 **키 기반 샤딩을 통한 수평 확장**으로 달성해야 한다. |

### 1.3 제출물 구성

```
.
├── README.md                          # 본 문서 (벤치마크 보고서)
├── 1_pipeline/
│   └── firewall_agg.conf              # Logstash 파이프라인 코드
├── 2_elk_config/
│   ├── docker-compose.yml             # 컨테이너 오케스트레이션 / 자원 격리
│   ├── logstash/
│   │   ├── logstash.yml               # Logstash 설정
│   │   └── es_template.json           # Elasticsearch 인덱스 템플릿
│   ├── metricbeat/
│   │   ├── metricbeat.yml             # Metricbeat 전역 설정
│   │   └── modules.d/
│   │       ├── docker.yml             # Docker 모듈 (컨테이너 지표)
│   │       └── logstash.yml           # Logstash 모듈 (파이프라인 지표)
│   └── logs/
│       └── invalid_format.log         # 형식 검증 실패 격리 로그 (4건)
├── 3_aggregate_output/
│   └── firewall_agg_result.csv.gz     # Worker 1 기준 agg 결과물 (552,904행, 7.3절 검증 완료)
├── 4_raw_metrics/                     # 측정 원본 데이터 (재현/검증용)
│   ├── worker{1,4,8}_eps.csv          # events.in / events.out
│   ├── worker{1,4,8}_logstash.csv     # Logstash CPU
│   ├── worker{1,4,8}_log_mem.csv      # Logstash Memory
│   ├── worker{1,4,8}_jvm_chart.csv    # JVM Heap
│   ├── worker{1,4,8}_disk.csv         # Logstash Disk I/O
│   └── worker{1,4,8}_es.csv           # Elasticsearch Disk Write / CPU
└── image/                             # 벤치마크 그래프 (21장)
```

> **`4_raw_metrics/`** 는 본 보고서의 모든 수치가 도출된 원본 시계열이다. Kibana Lens에서 10초 버킷 · Counter rate(per second)로 추출하였으며, 보고서의 모든 그래프와 표는 이 CSV로부터 직접 산출되었다. 검증을 원할 경우 이 데이터만으로 전 수치를 재현할 수 있다.

---

## 2. 테스트 환경

### 2.1 하드웨어 / OS

| 항목 | 사양 |
|---|---|
| CPU | AMD Ryzen 5 5600X (6 Core / 12 Thread) |
| Storage | NVMe M.2 SSD |
| Host OS | Windows + WSL2 |
| WSL2 할당 | `processors=10`, `memory=16GB`, `swap=0` |
| Container Runtime | Docker (WSL2 backend) |
| Elastic Stack | **8.11.0** (Elasticsearch / Logstash / Kibana / Metricbeat) |

> **과제 요구사항 "물리적 8Thread" 충족 여부:** 본 CPU는 12개 논리 스레드를 제공하며, WSL2에 10개를 노출하고 그중 8개를 Logstash 전용으로 할당하였다. 따라서 **Worker 8 테스트는 조정 없이 원안대로 수행**되었다.

### 2.2 컨테이너 자원 격리 (핵심 통제)

서비스 간 CPU 경합을 물리적으로 차단하기 위해 **`cpuset`으로 코어를 배타 할당**하였다.

| 서비스 | cpuset | CPU Limit | Memory Limit | JVM |
|---|:---:|:---:|:---:|:---:|
| **logstash** | **`0-7`** (전용 8스레드) | 8.0 | **8G** | `-Xms2g -Xmx2g` |
| elasticsearch | `8-9` | 1.5 | 4G | `-Xms2g -Xmx2g` |
| kibana | `8-9` | 0.3 | 1G | — |
| metricbeat | `8-9` | 0.2 | 1G | — |

> **Logstash가 8개 논리 스레드를 물리적으로 독점**하며, Elasticsearch·Kibana·Metricbeat는 나머지 2개 스레드에 격리된다. 이를 통해 Worker 8 테스트에서도 측정 대상과 관측 도구가 CPU를 두고 경합하지 않음을 보장하였다.

### 2.3 아키텍처

```
   test.log (550MB)
        │
        ▼
  ┌───────────────────────────────────────────┐
  │  Logstash  [cpuset 0-7 / 8GB / Heap 2GB]  │
  │                                           │
  │  file input (단일 스레드)                  │
  │        ↓                                  │
  │  형식 검증 → dissect → kv → mutate → date │
  │        ↓                                  │
  │  aggregate  (task_id: src+dst+port,       │
  │              timeout 20s, event-time)     │
  └───────────────────────────────────────────┘
        │ (HTTP Bulk)
        ▼
  ┌───────────────────────────────────────────┐
  │  Elasticsearch  [cpuset 8-9 / 4GB]        │
  │  firewall-agg-logs-*  (refresh_interval: -1) │
  └───────────────────────────────────────────┘
        ▲
        │  Metricbeat [cpuset 8-9]
        │   ├─ docker module  (container/cpu/diskio/memory, 5s)
        │   └─ logstash module (node/node_stats, 5s)
        ▼
     Kibana
```

**과제 선택항목 충족:**

- **Logstash Centralized Pipeline Management** — `xpack.management.enabled: true`, `pipeline.id: firewall_agg`로 Elasticsearch 기반 중앙 파이프라인 관리 적용
- **Metricbeat 연동 Logstash Metric 확보** — `logstash` 모듈의 `node`/`node_stats` metricset으로 파이프라인 내부 지표 수집

---

## 3. 측정 방법론

### 3.1 측정 대상 스코프

과제 명세의 `CPU / Mem / JVM / IO`는 측정 대상을 명시하지 않았다. 본 보고서는 다음과 같이 정의한다.

**주 지표 — Logstash 컨테이너/프로세스**
독립 변인이 `pipeline.workers`이며, 명세가 `Mem`과 `JVM`을 병기한 점을 근거로 *"단일 JVM 프로세스(Logstash)를 OS 계층(CPU/Memory)과 런타임 계층(JVM Heap)으로 분해하여 측정하라"* 는 의도로 해석하였다. Worker 수에 반응하지 않는 서비스는 주 지표에서 제외한다.

**보조 지표 — Elasticsearch 컨테이너**
Logstash 지표의 신뢰성은 다운스트림이 3개 테스트군에서 동일하게 거동했는지에 의존한다. 따라서 Elasticsearch의 CPU와 Disk Write를 함께 수집하여 4.5절에서 검증한다.

**IO의 정의**
관례적으로 "IO 사용량"은 Disk I/O를 지칭하므로 본 보고서의 `IO`는 **Disk I/O**를 의미한다. Network I/O 미측정 사유는 3.4절에 명시한다.

### 3.2 지표 수집 경로

| 지표 | 모듈 | 필드 | 집계 |
|---|---|---|---|
| CPU (Logstash) | docker | `docker.cpu.total.pct` | Average / Max |
| Memory (Logstash) | docker | `docker.memory.usage.total`, `.pct` | Average / Max |
| Disk I/O (Logstash) | docker | `docker.diskio.read.bytes`, `.write.bytes` | **Counter rate (per second)** |
| JVM Heap | logstash | `logstash.node.stats.jvm.mem.heap_used_in_bytes` | Average / Max |
| Events In/Out | logstash | `logstash.node.stats.events.in`, `.out` | **Counter rate (per second)** |
| CPU / Disk Write (ES) | docker | 상동 (`container.name: elasticsearch`) | 상동 |

- **Metricbeat 수집 주기:** `period: 5s` (docker·logstash 모듈 공통, 전 테스트군 동일)
- **Kibana 버킷:** `10초 고정` (Auto interval 사용 시 테스트 구간 길이에 따라 버킷 크기가 달라져 케이스 간 비교가 불가능해지므로 명시적으로 고정)
- **누적 카운터 처리:** `diskio`, `events` 계열은 컨테이너 시작 이후 누적값이므로 `Counter rate` + `Normalize by unit: per second`를 적용하여 초당 값으로 변환
- **그래프 렌더링:** 본 보고서의 모든 그래프는 위 경로로 추출한 원본 CSV(`4_raw_metrics/`)를 기반으로 렌더링하였다. Worker 간 비교 가능성을 확보하기 위해 **동일 지표는 세 케이스의 Y축 스케일을 통일**하였으며, 각 그래프의 음영은 파란색이 유입 구간, 주황색이 aggregate 드레인 구간을 나타낸다.

### 3.3 지표 정의 및 산출식

측정 구간의 정의에 따라 EPS 값이 크게 달라지므로, 본 보고서는 **세 가지를 모두 명시**한다.

```
① 유입 구간   = events.in  > 0 인 첫 버킷 ~ 마지막 버킷
② 드레인 구간 = 유입 종료 ~ events.out > 0 인 마지막 버킷
                (aggregate 타임아웃으로 잔여 맵이 flush되는 구간)
③ E2E 구간    = ① + ②  (첫 유입 ~ 마지막 인덱싱)

실효 EPS       = 총 이벤트 수 ÷ E2E 구간         ← 대표값
활성구간 EPS   = 총 이벤트 수 ÷ 유입 구간         ← 파이프라인 순수 처리 성능
1 minute EPS   = 10초 버킷 counter rate(per second)에 6버킷 이동평균 적용
                 → 해당 시리즈의 Avg / Min / Max
CPU 총 작업량  = Σ(버킷별 CPU pct × 10초)         ← 100만 건 처리에 소요된 CPU core-second
워커당 활용률  = 유입구간 평균 CPU ÷ pipeline.workers
```

> **1 minute EPS의 정의 근거:** 본 테스트의 최단 구간이 60초이므로 60초 버킷을 직접 사용하면 표본이 1개에 그쳐 Avg/Min/Max가 성립하지 않는다. 따라서 **10초 버킷 위에 6버킷(=60초) 이동평균**을 적용하여 "1분 EPS 시리즈"를 구성하고 그 통계를 산출하였다.

### 3.4 측정하지 못한 지표

| 지표 | 사유 | 대체 |
|---|---|---|
| **Network I/O** | Metricbeat `docker` 모듈의 metricset 구성에 `network`가 포함되지 않아 수집되지 않음 | Logstash → Elasticsearch 전송량은 `events.out` 및 Elasticsearch 컨테이너의 Disk Write로 간접 확인 |
| **JVM GC 상세** | Metricbeat `logstash` 모듈의 `node_stats` metricset은 GC collector 단위 지표를 노출하지 않음. `xpack.enabled: true`를 통한 `.monitoring-logstash-*` 수집 또는 `:9600/_node/stats/jvm` 직접 조회가 필요 | **JVM Heap 사용량(Avg/Max)** 으로 JVM 자원 사용을 평가. 전 구간 `-Xmx2g` 임계치 내 안정 확인 |

---

## 4. 벤치마크 변인 통제

### 4.1 인프라 자원 고정

`.wslconfig`로 WSL2에 노출되는 자원을 고정하고, Docker `cpuset`으로 서비스별 코어를 배타 할당하였다(2.2절).

```ini
[wsl2]
processors=10
memory=16GB
swap=0
localhostForwarding=true
```

`swap=0`은 스왑 발생에 따른 측정 왜곡을 원천 차단한다. JVM Heap은 `-Xms2g -Xmx2g`로 최소·최대를 동일하게 고정하여, 벤치마크 도중 힙 재할당 오버헤드를 배제하였다.

### 4.2 OS 페이지 캐시 격리

**문제:** 반복 테스트 시 550MB 로그 파일이 OS 페이지 캐시에 잔류하여, Logstash가 물리 디스크를 읽지 않고 메모리에서 데이터를 가져오는 캐시 히트가 발생한다. 이 경우 `Disk Read I/O`가 0에 가깝게 측정되는 왜곡이 발생한다.

**대응:** 매 테스트 시작 전 2단계로 캐시를 제거하였다.

```bash
# 1. 타겟 파일의 페이지 캐시를 정밀 퇴거
sudo vmtouch -e test.log

# 2. 시스템 전체 캐시(PageCache, dentries, inodes) 초기화
sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
```

**검증 결과:** 3개 테스트군 모두에서 Logstash 컨테이너의 **Disk Read 총량이 552.1~552.2MB로 원본 파일 크기와 일치**하였다. 이는 매 테스트가 캐시 개입 없이 물리 디스크로부터 전량을 읽었음을 정량적으로 입증한다.

| | Worker 1 | Worker 4 | Worker 8 |
|---|---:|---:|---:|
| Disk Read 총량 | **552.1 MB** | **552.2 MB** | **552.2 MB** |

### 4.3 Elasticsearch I/O 간섭 최소화

인덱싱 refresh 주기에 따른 디스크 I/O 스파이크가 Logstash 처리 성능 측정을 왜곡하지 않도록, 인덱스 템플릿에서 refresh를 비활성화하였다.

```json
{
  "index_patterns": ["firewall-agg-logs-*"],
  "template": {
    "settings": {
      "index.refresh_interval": "-1",
      "index.number_of_replicas": 0,
      "index.number_of_shards": 1
    }
  }
}
```

`replicas: 0`, `shards: 1`로 복제·분산에 따른 변인도 제거하였다.

### 4.4 단계적 서비스 안정화

Elasticsearch → Kibana → Logstash → Metricbeat 순으로 배포하고, 각 서비스의 초기 구동 부하(Startup Spike)가 안정화된 이후 로그 주입을 시작하였다. 실제로 CPU 시계열에서 **유입 시작 직전 구간의 CPU가 1~9% 수준**임을 확인하였다(6.3절 그래프).

### 4.5 통제하지 못한 변인 (한계)

| # | 변인 | 영향 | 완화 |
|:---:|---|---|---|
| 1 | **Windows 호스트 파일시스템 캐시** | WSL2 게스트 내 `drop_caches`는 게스트 레벨 캐시만 제거한다. 로그 파일이 위치한 ext4.vhdx의 블록을 Windows 호스트가 캐싱할 가능성은 통제 범위 밖이다. | 3개 테스트군에 동일 조건으로 적용되므로 **상대 비교의 공정성은 유지**된다. Disk Read 총량이 3케이스 모두 552MB로 일치하는 것이 이를 뒷받침한다. |
| 2 | **`refresh_interval: -1`** | 실 운영 환경과 다른 조건이다. 본 결과의 Out EPS 및 ES Disk Write를 실 운영 부하로 일반화할 수 없다. | Logstash 성능 측정이라는 목적상 다운스트림 변인 제거가 우선이며, 3케이스 동일 조건이다. |
| 3 | **SMT(Hyper-Threading)** | `cpuset 0-7`은 논리 스레드 8개이며, 물리 코어로는 4~6개에 대응한다. Worker 8은 SMT 페어링된 스레드에서 실행된다. | 3케이스 동일 조건. 다만 Worker 8의 병렬 효율 저하에 SMT가 기여했을 가능성은 배제할 수 없다. |
| 4 | **Elasticsearch 부하 차이** | Worker 수가 늘수록 ES에 데이터가 더 빠르게 유입되어 ES CPU가 14.5% → 18.7% → 23.2%로 상승한다. 이는 통제 실패가 아니라 처리량 증가의 자연스러운 결과다. | ES CPU 최대치가 할당 한도(1.5 core = 150%)의 **63% 수준**에 그쳐, ES가 백프레셔를 유발했을 가능성은 낮다(6.6절). |

---

## 5. 파이프라인 구현

### 5.1 파싱 명세

Syslog(RFC 5424) 헤더 + Key-Value 본문 구조의 FortiGate 방화벽 로그를 5단계로 처리한다.

```
입력 예시:
<14>1 2026-02-04T00:00:00.042140Z [fw4_deny] [10.0.0.255] start_time="..." end_time="..."
src_ip=10.0.0.134 src_port=60427 dst_ip=100.100.100.106 dst_port=443 protocol=6 ...
packets_total=1 bytes_total=100 ...
```

| 단계 | 필터 | 처리 내용 |
|:---:|---|---|
| 1 | 조건문 | Syslog 표준 형식(`^<\d+>`) 검증 |
| 2 | `dissect` | 헤더 분해 → `syslog_pri`, `syslog_ver`, `syslog_timestamp`, `log_action`, `device_ip`, `kv_data` |
| 3 | `kv` | 본문 Key-Value 파싱 (`field_split: " "`, `value_split: "="`) |
| 4 | `mutate` | `src_port`/`dst_port`/`bytes_total`/`packets_total` 정수 변환 + 음수 검증 |
| 5 | `date` → `aggregate` | `end_time`을 `@timestamp`로 파싱(UTC) 후 20초 윈도우 병합 |

**Aggregate 출력 스키마**

| 필드 | 설명 |
|---|---|
| `src_ip`, `dst_ip`, `dst_port` | 병합 키 (task_id 구성) |
| `protocol`, `action` | 세션 속성 |
| `first_start_time`, `last_end_time` | 윈도우 내 최초/최종 시각 |
| `sum_bytes_total`, `sum_packets_total` | 합계 집계 |
| `event_count` | 윈도우에 병합된 원본 로그 수 |

> **`dissect` 채택 근거:** `grok`은 정규식 백트래킹으로 CPU를 크게 소모한다. 본 로그는 구분자가 고정된 구조이므로 `dissect`(단순 위치 기반 분할)로 파싱하여 필터 단계의 CPU 오버헤드를 최소화하였다. 이는 Worker 변인의 효과를 더 선명하게 관측하기 위한 설계 선택이기도 하다.

### 5.2 오류 감지 및 격리 (선택 항목)

파이프라인은 **각 파싱 단계마다 실패를 원인별로 태깅하고, 별도 파일로 분리 격리**한다. 오류 데이터가 aggregate 단계로 유입되어 통계를 오염시키는 것을 원천 차단하는 구조다.

```ruby
if [message] !~ /^<\d+>/ { mutate { add_tag => ["_error_format", "_error"] } }

if "_error" not in [tags] {
  dissect { ... tag_on_failure => ["_error_dissect", "_error"] }
}
if "_error" not in [tags] {
  kv { ... tag_on_failure => ["_error_kv", "_error"] }
}
if "_error" not in [tags] {
  mutate { convert => { ... } }
  if [bytes_total] < 0 or [packets_total] < 0 {
    mutate { add_tag => ["_error_data", "_error"] }
  }
}
```

```ruby
output {
  if      "_error_format"  in [tags] { file { path => ".../invalid_format.log"  codec => json_lines } }
  else if "_error_dissect" in [tags] { file { path => ".../failed_dissect.log"  codec => json_lines } }
  else if "_error_kv"      in [tags] { file { path => ".../failed_kv.log"       codec => json_lines } }
  else if "_error_data"    in [tags] { file { path => ".../invalid_data.log"    codec => json_lines } }
  else if "aggregated"     in [tags] { elasticsearch { ... } }
}
```

**격리 실적**

| 태그 | 격리 파일 | 건수 |
|---|---|---:|
| `_error_format` | `invalid_format.log` | **4** |
| `_error_dissect` | `failed_dissect.log` | 0 |
| `_error_kv` | `failed_kv.log` | 0 |
| `_error_data` | `invalid_data.log` | 0 |

**원인 분석 — 입력 파일 압축 해제 방식에 기인한 아카이브 메타데이터 혼입**

격리된 4건은 방화벽 로그가 아니라 **tar 아카이브의 메타데이터**였다. 제공된 로그 파일은 `.tar.xz` 형태였는데, **xz 압축만 해제하고 tar 아카이브를 그대로 입력**한 결과 tar 헤더가 로그 라인으로 읽혔다.

```
generated_forti_logs.tar.xz
   │
   ├─ xz -d 만 수행        →  generated_forti_logs.tar     ← 본 벤치마크의 입력 (test.log)
   │                          tar 헤더 블록이 라인으로 혼입 → 1,000,003 라인
   │
   └─ tar -xJf 로 완전 해제 →  generated_forti_logs.txt
                              순수 방화벽 로그 → 1,000,000 라인 (검증 완료)
```

| # | 격리된 내용 | 정체 |
|:---:|---|---|
| 1 | `._generated_forti_logs.txt` + `ustar` + `Mac OS X` + `com.apple.provenance` | AppleDouble 리소스 포크 + tar 헤더 블록 |
| 2 | `generated_forti_logs.txt` + `ustar` + `<14>1 2026-02-04T00:00:00.042140Z [fw4_deny] ...` | tar 헤더 블록 + **실제 로그 1건이 결합된 라인** |
| 3 | `49 SCHILY.xattr.com.apple.provenance=...` | PAX 확장 헤더 |
| 4 | `57 LIBARCHIVE.xattr.com.apple.provenance=AQAA3GUGQNLKspg` | PAX 확장 헤더 |

**데이터 정합성 재구성**

```
test.log (tar 아카이브)                                    1,000,003 라인
  ├─ tar/PAX 메타데이터 (#1, #3, #4)                               3 라인   → 격리
  ├─ tar 헤더 + 실제 로그 1건 결합 (#2)                            1 라인   → 격리
  └─ 정상 방화벽 로그                                        999,999 라인   → aggregate
                                                            ─────────────
원본 방화벽 로그 총량 = 999,999 + (#2에 결합된 1건) =        1,000,000 건  ✅
```

**tar -xJf로 완전 해제한 파일이 정확히 1,000,000 라인임을 별도 검증하였다.** 즉 제공 데이터는 온전하며, 4건의 격리는 **입력 파일 준비 단계의 문제**였다.

**본 벤치마크의 유효성에 미치는 영향**

| 항목 | 영향 |
|---|---|
| **성능 지표 (EPS/CPU/Memory/JVM/Disk)** | **없음.** tar 헤더는 약 3KB로 전체 550MB의 0.0006% 미만이다. 세 테스트군이 동일 입력을 사용했으므로 Worker 간 비교의 공정성도 완전히 유지된다. |
| **Aggregate 정합성** | **없음.** `sum(event_count) = 999,999`가 3케이스 전부 일치하며, 격리된 4건은 aggregate 단계에 진입하지 않았다. |
| 원본 로그 커버리지 | 999,999 / 1,000,000 = **99.9999%.** #2 라인에 결합된 로그 1건(`src_ip=10.0.0.134`, `dst_ip=100.100.100.106`, `dst_port=443`)만 집계에서 제외되었다. |

**의의:** 이 사례는 파이프라인의 1단계 형식 검증(`^<\d+>`)이 **의도치 않은 입력 오염을 파싱 시도 이전에 100% 차단**했음을 실증한다. 검증 로직이 없었다면 tar 헤더의 바이너리 문자열이 `dissect`/`kv` 단계로 유입되어 예외를 발생시키거나, 최악의 경우 잘못된 필드로 파싱되어 통계를 오염시켰을 것이다. `dissect`·`kv`·`date` 단계의 실패 건수가 모두 0인 것은, 정상 형식의 로그가 **100% 파싱 오류 없이 처리**되었음을 의미하며, 이로써 과제의 **"파싱 오류 없는 로그 데이터 처리 파이프라인 구축(필수)"** 요건을 충족한다.

> **파이프라인 개선 제안:** 현재 격리 로직은 `file` 출력 기반이다. 운영 환경에서는 Logstash의 **Dead Letter Queue(DLQ)** 또는 별도 Elasticsearch 인덱스(`firewall-error-*`)로 라우팅하고, 태그별 건수에 Kibana Alert을 연결하여 **오류율 급증 시 즉시 탐지**하는 구조를 권장한다.

### 5.3 Aggregate 설정 (필수 항목)

```ruby
aggregate {
  # [필수] source.ip, destination.ip, destination.port 를 key 로 사용
  task_id => "%{src_ip}_%{dst_ip}_%{dst_port}"

  # [필수] 로그 event time 으로 timeout 20초 설정
  timeout => 20
  timeout_timestamp_field => "@timestamp"

  timeout_tags => ['aggregated']
  push_map_as_event_on_timeout => true

  code => "
    map['src_ip']  ||= event.get('src_ip')
    map['dst_ip']  ||= event.get('dst_ip')
    map['dst_port']||= event.get('dst_port')
    map['protocol']||= event.get('protocol')
    map['action']  ||= event.get('log_action')

    map['first_start_time'] ||= event.get('start_time')
    map['last_end_time']      = event.get('end_time')

    map['sum_bytes_total']   ||= 0
    map['sum_bytes_total']    += event.get('bytes_total').to_i
    map['sum_packets_total'] ||= 0
    map['sum_packets_total']  += event.get('packets_total').to_i
    map['event_count']       ||= 0
    map['event_count']        += 1
  "
}

# 집계에 반영된 원본 개별 로그는 폐기 (통계 이벤트만 잔존)
if "_error" not in [tags] and "aggregated" not in [tags] { drop {} }
```

**요구사항 충족 확인**

| 요구사항 | 구현 | 충족 |
|---|---|:---:|
| `source.ip`, `destination.ip`, `destination.port`를 key로 사용 | `task_id => "%{src_ip}_%{dst_ip}_%{dst_port}"` | ✅ |
| 로그 event time으로 timeout 20초 설정 | `timeout => 20` + `timeout_timestamp_field => "@timestamp"` | ✅ |

> `timeout_timestamp_field`를 지정하지 않으면 aggregate는 **시스템 시계(wall-clock)** 를 기준으로 만료를 판정한다. 이 경우 로그의 실제 발생 시각과 무관하게 "Logstash가 처리한 시각"으로 윈도우가 잘리므로, 재현성이 없고 과제 요구사항에도 위배된다. 본 파이프라인은 `date` 필터로 `end_time`을 `@timestamp`에 매핑한 뒤 이를 기준 시계로 사용하여, **처리 속도와 무관하게 로그 내부 시간축을 기준으로 20초 윈도우를 구성**한다.

---

## 6. 벤치마크 결과

### 6.1 종합 지표표

| 분류 | 항목 | **Worker 1** | **Worker 4** | **Worker 8** |
|:---|:---|---:|---:|---:|
| **처리 속도** | 유입 시작 시각 | 14:51:00 | 16:01:50 | 16:23:00 |
| | 유입 종료 시각 | 14:54:20 | 16:02:50 | 16:23:50 |
| | 인덱싱 종료 시각 | 14:54:50 | 16:03:10 | 16:24:20 |
| | **유입 소요 시간** | **210초** | **70초** | **60초** |
| | 드레인 구간 | 30초 | 20초 | 30초 |
| | **E2E 총 소요 시간** | **240초** | **90초** | **90초** |
| | 실효 In EPS (E2E 기준) | 4,167 /s | 11,111 /s | 11,111 /s |
| | 활성구간 In EPS | 4,762 /s | 14,286 /s | **16,667 /s** |
| | 1min In EPS — Avg | 4,374 /s | 11,785 /s | **12,947 /s** |
| | 1min In EPS — Min | 1,000 /s | 200 /s | 200 /s |
| | 1min In EPS — Max | 5,469 /s | 16,633 /s | **18,516 /s** |
| | 실효 Out EPS (E2E 기준) | 2,304 /s | 6,118 /s | 5,953 /s |
| | 1min Out EPS — Avg | 2,405 /s | 6,441 /s | 6,885 /s |
| | 1min Out EPS — Min | 451 /s | 11 /s | 0 /s |
| | 1min Out EPS — Max | 3,026 /s | 9,133 /s | **9,859 /s** |
| **CPU** | 유입구간 평균 | 94.42 % | 255.04 % | 356.26 % |
| | E2E 평균 | 82.70 % | 198.51 % | 237.87 % |
| | 최대 | 129.20 % | 315.55 % | **455.60 %** |
| | **워커당 활용률** | **94.4 %** | **63.8 %** | **44.5 %** |
| | **CPU 총 작업량** | 198.5 core·s | **178.7 core·s** ⭐ | 214.1 core·s |
| | 1 core·s당 처리량 | 5,038 events | **5,596 events** ⭐ | 4,671 events |
| **Memory** | usage 평균 | 1,896.6 MB | 1,874.0 MB | 1,923.4 MB |
| | usage 최대 | 1,952.4 MB | 1,950.7 MB | 2,020.8 MB |
| | usage 평균 (%, 분모 8GiB) | 23.14 % | 22.86 % | 23.47 % |
| | usage 최대 (%) | 23.80 % | 23.80 % | 24.70 % |
| **JVM** | Heap 평균 | 860.7 MB | 791.0 MB | 1,005.7 MB |
| | Heap 최대 | 1,321.9 MB | 1,055.4 MB | 1,279.6 MB |
| | Heap 최대 (%, 분모 2GB) | 64.5 % | 51.5 % | 62.5 % |
| **Disk I/O** | Read Peak (Logstash) | 4.00 MB/s | 13.60 MB/s | 12.80 MB/s |
| | **Read 총량 (Logstash)** | **552.1 MB** | **552.2 MB** | **552.2 MB** |
| | Write Peak (Logstash) | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s |
| | Write Peak (Elasticsearch) | 3.82 MB/s | 8.22 MB/s | 7.14 MB/s |
| | Write 총량 (Elasticsearch) | 403.1 MB | 357.6 MB | 348.0 MB |
| | ES CPU 평균 / 최대 | 14.5 % / 51.4 % | 18.7 % / 95.4 % | 23.2 % / 75.1 % |
| **데이터 처리량** | In Data Count | 1,000,003 | 1,000,003 | 1,000,003 |
| | 오류 격리 | 4 | 4 | 4 |
| | Aggregate 입력 | 999,999 | 999,999 | 999,999 |
| | **Out Data Count** | **552,908** | **550,642** | **535,756** |
| | Agg 윈도우 수 | 552,904 | 550,638 | 535,752 |
| | **sum(event_count)** | **999,999** | **999,999** | **999,999** |
| | **축소율** | **44.71 %** | **44.94 %** | **46.42 %** |

> **수집 데이터 신뢰성 검증:** 10초 버킷 `counter rate`를 적분한 값이 파이프라인 카운터와 정확히 일치함을 확인하였다.
> `Σ(events.in rate × 10s) = 1,000,003` (3케이스 전부), `Σ(events.out rate × 10s) = 552,908 / 550,642 / 535,756`. **오차 0건.**

### 6.2 처리 속도 (EPS)

#### [Worker 1 — Baseline]
![EPS Worker 1](./image/worker1_eps.png)

#### [Worker 4 — Multi-Thread]
![EPS Worker 4](./image/worker4_eps.png)

#### [Worker 8 — Max Load]
![EPS Worker 8](./image/worker8_eps.png)

**분석 — 유입은 계속 빨라졌으나, E2E는 W4에서 멈췄다**

![E2E 시간 분해](./image/comparison_timeline.png)

| | 유입 소요 | 배속 | 드레인 | E2E | 배속 | **드레인이 E2E에서 차지하는 비중** |
|:---:|---:|---:|---:|---:|---:|---:|
| W1 | 210초 | 1.00× | 30초 | 240초 | 1.00× | **12.5 %** |
| W4 | 70초 | **3.00×** | 20초 | 90초 | 2.67× | **22.2 %** |
| W8 | 60초 | **3.50×** | 30초 | 90초 | 2.67× | **33.3 %** |

Worker 8은 Worker 4보다 **느리지 않다.** 유입 처리를 70초 → 60초로 **14% 더 빠르게** 완료했다. 그럼에도 E2E가 90초로 동일한 이유는 다음과 같다.

> **`aggregate` 필터의 20초 타임아웃 드레인은 Worker 수와 무관한 고정 오버헤드다.** 마지막 이벤트가 유입된 후, 잔여 맵이 event-time 기준 20초 타임아웃에 도달하여 flush되기까지 20~30초가 소요된다. Worker 8이 유입에서 절약한 10초는 이 드레인 구간에서 그대로 상쇄되었다.

이 고정 오버헤드가 E2E에서 차지하는 비중은 **12.5% → 22.2% → 33.3%** 로 급격히 증가한다. **유입 처리가 빨라질수록 드레인의 지배력이 커지므로, Worker 증설의 한계 효용은 구조적으로 0에 수렴한다.**

**Aggregate 축소 효과 검증**

유출 건수는 W1 552,908건(**44.71% 축소**), W4 550,642건(**44.94% 축소**), W8 535,756건(**46.42% 축소**)이었다. 전 케이스에서 원본 이벤트가 정상 병합되어 Elasticsearch 인덱싱 부하와 스토리지 사용량을 절반 가까이 절감하였다.

### 6.3 CPU 사용률

#### [Worker 1 — Baseline]
![CPU Worker 1](./image/worker1_cpu.png)

#### [Worker 4 — Multi-Thread]
![CPU Worker 4](./image/worker4_cpu.png)

#### [Worker 8 — Max Load]
![CPU Worker 8](./image/worker8_cpu.png)

**분석 ① — 측정 구간이 깨끗하다**

세 케이스 모두 유입 시작과 동시에 즉시 plateau에 도달하고, 유입 종료와 동시에 1% 미만으로 낙하하는 **구형파(square wave) 패턴**을 보인다. 램프업/테일이 없다는 것은 4.4절의 단계적 안정화가 성공했으며, 측정 구간에 기동 부하나 잔여 작업이 섞이지 않았음을 의미한다.

**분석 ② — 워커당 CPU 활용률의 붕괴 (핵심 지표)**

![CPU 효율 비교](./image/comparison_cpu.png)

| | 유입구간 평균 CPU | 코어 환산 | 할당 코어 | **워커당 활용률** |
|:---:|---:|---:|:---:|---:|
| **W1** | 94.42 % | 0.94 core | 8 | **94.4 %** |
| **W4** | 255.04 % | 2.55 core | 8 | **63.8 %** |
| **W8** | 356.26 % | 3.56 core | 8 | **44.5 %** |

Worker 1이 **정확히 1개 코어를 채우는 것**은 `pipeline.workers=1`의 교과서적 동작이다. 그러나 Worker를 8배 늘렸을 때 CPU 사용량은 **3.8배만 증가**했고, 워커당 활용률은 **94.4% → 44.5%로 반토막**났다.

> Worker를 아무리 늘려도 각 워커가 자기 몫의 CPU를 쓰지 못한다는 것은, **워커들이 CPU를 기다리는 것이 아니라 서로를 기다리고 있음(lock contention)** 을 의미한다. 이는 `aggregate` 필터가 파이프라인 레벨 공유 맵을 mutex로 보호하며, 그 구간이 사실상 직렬 실행되기 때문이다(8장 참조).

**분석 ③ — CPU 총 작업량은 W4가 최적**

| | CPU 총 작업량 | 1 core·s당 처리 | W4 대비 |
|:---:|---:|---:|---:|
| W1 | 198.5 core·s | 5,038 events | +11.1 % |
| **W4** | **178.7 core·s** ⭐ | **5,596 events** | 기준 |
| W8 | 214.1 core·s | 4,671 events | **+19.8 %** |

**Worker 8은 Worker 4 대비 CPU를 19.8% 더 소모하고, E2E 시간은 0% 개선했다.** Worker 1이 W4보다 11.1% 비효율적인 것은 파이프라이닝 이득을 못 얻기 때문이고, Worker 8이 19.8% 비효율적인 것은 경합 오버헤드 때문이다. **W4가 U자 곡선의 저점이다.**

**분석 ④ — CPU는 포화되지 않았다**

Worker 8의 유입구간 평균은 3.56 코어, 최대 4.56 코어로, **할당된 8개 논리 스레드의 44.5~57%만 사용**하였다. CPU 자원이 절반 이상 남아있는 상태에서 처리량이 늘지 않았다는 사실 자체가, 병목이 CPU가 아님을 증명한다.

### 6.4 컨테이너 메모리

#### [Worker 1 — Avg: 1,896.6 MB (23.14%)]
![MEM Worker 1](./image/worker1_mem.png)

#### [Worker 4 — Avg: 1,874.0 MB (22.86%)]
![MEM Worker 4](./image/worker4_mem.png)

#### [Worker 8 — Avg: 1,923.4 MB (23.47%)]
![MEM Worker 8](./image/worker8_mem.png)

**분석**

컨테이너 메모리 사용량은 세 케이스 모두 **1,874~1,923 MB (할당 8GiB의 22.9~23.5%)** 로 완전히 평탄하다. 최대치도 2,020.8 MB(24.70%)를 넘지 않았다.

- **Worker 수와 메모리 사용량은 무관하다.** Worker를 8배 늘려도 메모리는 2.6%만 증가했다. Logstash의 워커는 스레드이므로 힙을 공유하며, `aggregate` 맵 또한 파이프라인 단위 공유 자원이기 때문이다.
- **메모리 누수 없음.** 초당 16,000건이 유입되는 W8 구간에서도 사용량이 수평선(Plateau)을 유지했다.
- 사용률 백분율의 분모는 `deploy.resources.limits.memory: 8G`이며, 실측 데이터에서 역산한 값(8.004 GiB)이 설정값과 일치함을 확인하였다. 즉 **컨테이너 메모리 제한이 실제로 적용되고 있다.**

### 6.5 JVM Heap

#### [Worker 1 — Avg: 860.7 MB / Max: 1,321.9 MB]
![JVM Worker 1](./image/worker1_jvm.png)

#### [Worker 4 — Avg: 791.0 MB / Max: 1,055.4 MB]
![JVM Worker 4](./image/worker4_jvm.png)

#### [Worker 8 — Avg: 1,005.7 MB / Max: 1,279.6 MB]
![JVM Worker 8](./image/worker8_jvm.png)

**분석**

JVM Heap은 전 구간에서 **평균 791~1,006 MB, 최대 1,055~1,322 MB**로, `-Xmx2g` 임계치의 **51.5~64.5%** 내에서 관리되었다.

- **W8의 평균 Heap이 가장 높다(1,005.7 MB).** 8개 워커가 동시에 배치를 보유하면서 in-flight 객체가 증가한 결과이며, 예상되는 거동이다.
- **W1의 최대 Heap이 가장 높다(1,321.9 MB).** 처리 시간이 210초로 길어 동시에 살아있는 aggregate 맵의 수명이 길었기 때문으로 해석된다.
- **어느 케이스도 임계치에 근접하지 않았다.** 톱니 패턴이 안정적으로 유지되어 GC가 정상 작동했음을 보여주며, Heap 부족으로 인한 성능 저하는 발생하지 않았다.

> **주의:** 본 데이터셋은 활성 key 수가 제한적이다. 실 운영에서 key cardinality가 폭증하면 aggregate 맵이 힙을 선형적으로 잠식하므로, Heap 여유가 곧 안전을 의미하지는 않는다(8장 한계 #5).

### 6.6 Disk I/O

#### [Worker 1 — Read Peak: 4.00 MB/s]
![IO Worker 1](./image/worker1_disk.png)

#### [Worker 4 — Read Peak: 13.60 MB/s]
![IO Worker 4](./image/worker4_disk.png)

#### [Worker 8 — Read Peak: 12.80 MB/s]
![IO Worker 8](./image/worker8_disk.png)

#### [Elasticsearch — Disk Write & CPU (Worker 1 / 4 / 8)]
![ES Worker 1](./image/worker1_es.png)
![ES Worker 4](./image/worker4_es.png)
![ES Worker 8](./image/worker8_es.png)

**분석 ① — Read: 캐시 통제 성공, 그러나 포화는 아니다**

| | Read Peak | Read 총량 |
|:---:|---:|---:|
| W1 | 4.00 MB/s | 552.1 MB |
| W4 | 13.60 MB/s | 552.2 MB |
| W8 | 12.80 MB/s | 552.2 MB |

Read 총량이 3케이스 모두 원본 파일 크기와 일치하여, 4.2절의 캐시 제거 통제가 성공했음을 입증한다. Peak는 W1 → W4에서 3.4배 상승했고, W8은 W4와 오차 범위 내로 동일했다.

> **13 MB/s를 "디스크 대역폭 포화"로 해석해서는 안 된다.** 본 환경의 NVMe M.2 SSD는 순차 읽기 수천 MB/s급이며, **13 MB/s는 그 1% 미만**이다. Disk Read는 **수요 종속 지표**이므로, 파이프라인이 그만큼만 요구했다는 의미일 뿐 병목의 근거가 될 수 없다. W4·W8의 Read Peak가 유사한 것은 두 케이스의 파일 소비 속도가 비슷했다는 사실의 반영이다.

**분석 ② — Logstash Write 0MB/s의 정확한 해석**

| | Logstash Write | **Elasticsearch Write** |
|:---:|---:|---:|
| W1 | 0.00 MB/s (총 0.2 MB) | **3.82 MB/s (총 403.1 MB)** |
| W4 | 0.00 MB/s (총 0.2 MB) | **8.22 MB/s (총 357.6 MB)** |
| W8 | 0.00 MB/s (총 0.2 MB) | **7.14 MB/s (총 348.0 MB)** |

Logstash 컨테이너의 Disk Write가 0인 것은, Logstash가 파일을 읽어 **인메모리에서 aggregate 연산을 수행한 뒤 즉시 HTTP Bulk로 전송**하며, Persistent Queue(PQ)나 DLQ를 사용하지 않기 때문이다.

> **다만 이를 "스택 전체에 쓰기 부하가 없다"로 해석해서는 안 된다.** 같은 구간에 Elasticsearch 컨테이너는 **348~403 MB를 실제로 기록**했다(translog + segment). 즉 쓰기 부하는 사라진 것이 아니라 **Logstash에서 Elasticsearch로 이동**한 것이며, 본 측정의 스코프가 Logstash 컨테이너였기에 0으로 관측된 것이다. 또한 4.3절의 `refresh_interval: -1`로 쓰기가 의도적으로 억제된 조건임을 함께 고려해야 한다.

**분석 ③ — Elasticsearch는 병목이 아니다**

ES CPU는 W1 14.5% → W4 18.7% → W8 23.2%(평균), 최대 95.4%로 상승했으나, 이는 **할당 한도 1.5 core(=150%)의 63% 수준**이다. ES가 포화되어 백프레셔를 유발했을 가능성은 낮으며, W4 → W8의 E2E 정체를 다운스트림으로 설명할 수 없다.

---

## 7. Aggregate 정합성 검증

### 7.1 In / Out / Sum 검증

과제 요구사항: *"agg 적용된 결과의 경우 out count는 축소되고, sum 집계 count 는 동일해야함"*

```
                                       Worker 1     Worker 4     Worker 8
─────────────────────────────────────────────────────────────────────────
원본 방화벽 로그 (tar -xJf 검증)      1,000,000    1,000,000    1,000,000
In Data Count (events.in, tar 포함)   1,000,003    1,000,003    1,000,003
  ├─ 형식 검증 실패 → 격리 (5.2절)            4            4            4
  └─ Aggregate 파이프라인 유입            999,999      999,999      999,999
                                       ─────────    ─────────    ─────────
Out Data Count (events.out)             552,908      550,642      535,756
  = Agg 윈도우 수 + 격리 이벤트 4건

sum(event_count)                        999,999      999,999      999,999   ✅ 전 케이스 동일
축소율                                    44.71%       44.94%       46.42%   ✅ 축소 확인
```

| 검증 항목 | 결과 |
|---|:---:|
| **out count 축소** | ✅ 999,999 → 552,904 / 550,638 / 535,752 (44.7~46.4% 축소) |
| **sum 집계 count 동일** | ✅ **999,999로 3케이스 전부 일치** — Worker 수와 무관하게 총량 완전 보존 |
| 파싱 오류 | ✅ `dissect`/`kv`/`date` 실패 0건 |

**해석:** `events.out` 카운터는 파이프라인을 빠져나간 모든 이벤트를 집계하므로, `file` 출력으로 격리된 오류 4건이 포함된다. 순수 aggregate 윈도우 수는 `events.out − 4`이다.

**Worker 수가 sum에 영향을 주지 않는 이유:** `aggregate` 필터의 `map['sum_bytes_total'] += ...` 연산은 mutex로 보호되는 임계 구역에서 수행되므로, 병렬 실행 여부와 무관하게 **덧셈의 원자성과 총합이 보존**된다. 즉 Worker 증설은 **총량(sum)은 깨지 않지만 경계(윈도우)는 깬다** — 이것이 7.2절의 핵심이다.

### 7.2 윈도우 이벤트 밀도 분포

전체 이벤트 총합이 보존되더라도, 20초 윈도우 하나에 몇 개의 로그가 병합되었는지의 **분포**가 Worker 환경에 따라 어떻게 달라지는지 분석하였다.

![윈도우 밀도 분포](./image/comparison_density.png)

| 윈도우 내 이벤트 수 | **Worker 1** (기준) | **Worker 4** | **Worker 8** | 증감 (W1→W4 / W1→W8) |
|:---:|---:|---:|---:|---:|
| **1개** | 230,526 | 227,838 | 211,073 | **−2,688 / −19,453** |
| **2개** | 225,463 | 224,678 | 218,600 | **−785 / −6,863** |
| **3개** | 74,550 | 75,275 | 79,574 | **+725 / +5,024** |
| **4개** | 17,902 | 18,250 | 20,834 | **+348 / +2,932** |
| **5~8개** | 4,460 | 4,596 | 5,667 | **+136 / +1,207** |
| **9~10개** | 3 | 5 | 8 | **+2 / +5** |
| **총 윈도우 수** | **552,904** | **550,638** | **535,752** | **−2,266 / −17,152** |

> ※ Worker 1의 분포 합계(552,904)는 `events.out`(552,908)에서 오류 격리 4건을 제외한 값과 정확히 일치한다.

**변화 패턴**

```
소규모 윈도우(1~2개)      W4:  −3,473개        W8:  −26,316개    (7.6배 심화)
중대형 윈도우(3개 이상)   W4:  +1,209개        W8:   +9,163개    (7.6배 심화)
총 윈도우 수              W4:  −2,266개        W8:  −17,152개    (7.6배 심화)
```

**메커니즘 분석 — 기준 시계(Reference Clock)의 역행**

`timeout_timestamp_field => "@timestamp"` 설정 시, `aggregate` 필터는 시스템 시계가 아니라 **처리 중인 이벤트의 timestamp를 기준 시계로 삼아** 맵의 만료를 판정한다.

- **Worker 1:** `file` input이 파일을 순차적으로 읽고 단일 워커가 순서대로 처리하므로, 기준 시계가 **단조 증가**한다. 20초가 경과하면 맵이 정확히 만료된다.
- **Worker 4·8:** 이벤트가 배치 단위로 여러 워커에 분배되어 **처리 순서가 역전**된다. 앞선 timestamp를 가진 이벤트가 나중에 처리되면 **기준 시계가 뒤로 밀리고**, 만료되었어야 할 맵이 존속한다. 이 맵은 다음 윈도우에 속했어야 할 이벤트까지 흡수하여 **과병합(over-aggregation)** 이 발생한다.

관측된 "**소규모 윈도우 감소 + 중대형 윈도우 증가 + 총 윈도우 수 감소**" 패턴은 이 메커니즘과 정확히 일치하며, 왜곡의 정도가 Worker 수에 비례하여 **7.6배로 심화**되었다.

**보안 운영 관점의 리스크**

정보보호 모니터링에서 20초 세션 밀도는 **포트 스캔, 브루트포스, C2 비콘 탐지**의 1차 지표로 사용된다. Worker 8 환경에서는 단일 세션(1~2건)이 기준 대비 26,316개 사라지고 중대형 세션이 9,163개 증가했다. 이는 **"드물게 발생한 단발성 접근"이 "빈번한 연결 시도"로 재분류**되었음을 의미하며, 임계치 기반 탐지 룰의 오탐·미탐을 직접적으로 유발한다. **속도를 위한 Worker 증설이 탐지 신뢰도를 훼손하는 구조**다.

> **추가 검증 제안:** aggregate `code` 블록에 `map['span_sec'] = map['last_end_time'] - map['first_start_time']` 를 추가하면, `span_sec > 20`인 윈도우 수를 Worker별로 집계할 수 있다. Worker 1에서 0건, Worker 4·8에서 증가한다면 위 메커니즘이 정량적으로 확정된다.


### 7.3 제출 결과물 직접 검증

`3_aggregate_output/firewall_agg_result.csv.gz` (Worker 1 기준)를 원본 데이터로 직접 검증하였다.

| 검증 항목 | 실측값 | 기대값 | 판정 |
|---|---:|---:|:---:|
| 레코드 수 | **552,904** | `events.out`(552,908) − 격리 4건 | ✅ |
| `sum(event_count)` | **999,999** | aggregate 유입 건수와 동일 | ✅ |
| `sum(sum_bytes_total)` | 99,999,900 | — | — |
| `sum(sum_packets_total)` | 999,999 | `event_count` 합과 일치 | ✅ |
| 고유 key 수 (3-tuple) | 2,550 | — | — |
| `max(event_count)` | **10** | 윈도우당 이벤트 상한 | ✅ |
| **윈도우 시간 폭 최대** | **20초** | `timeout => 20` | ✅ |
| **20초 초과 윈도우** | **0건 (0.00%)** | 0건 | ✅ **완전 충족** |

> **핵심 검증:** `last_end_time − first_start_time`이 **정확히 20초에서 상한**되며 초과 레코드가 **0건**이다. 이는 `timeout_timestamp_field => "@timestamp"` 설정이 의도대로 작동하여, **시스템 시계가 아닌 로그 내부의 event time을 기준으로 20초 윈도우가 구성**되었음을 결과물 자체로 입증한다.
>
> 대조 실험으로, `timeout_timestamp_field`를 지정하지 않은 실행의 결과물은 윈도우 시간 폭이 최대 **593초(데이터 전체 구간)** 에 달하고 **42.5%의 레코드가 20초를 초과**하며 `max(event_count)`가 **140**까지 치솟았다. 이는 8.1절에서 서술한 기준 시계(reference clock)의 동작 차이를 실증적으로 확인해 준다.

---

## 8. Aggregate Filter 작동 원리와 한계

### 8.1 작동 원리

| # | 단계 | 동작 |
|:---:|---|---|
| 1 | **task_id 평가** | 이벤트마다 `task_id` 표현식을 평가하여 병합 키를 산출한다. 본 파이프라인은 `%{src_ip}_%{dst_ip}_%{dst_port}` 를 사용한다. |
| 2 | **맵 조회/생성** | 해당 key의 맵이 없으면 생성, 있으면 조회한다. 맵은 `@@aggregate_maps`라는 **파이프라인 레벨 클래스 변수**에 보관된다. |
| 3 | **code 실행** | Ruby `code` 블록이 실행되어 맵을 갱신한다. **맵 접근 전체가 mutex로 보호**되어 동시 접근이 직렬화된다. |
| 4 | **만료 판정** | `timeout => 20` + `timeout_timestamp_field => "@timestamp"` 설정 시, **처리 중인 이벤트의 timestamp를 기준 시계**로 삼아 맵 생성 시각으로부터 20초 경과 여부를 판정한다. |
| 5 | **이벤트 발행** | 만료된 맵은 evict되며, `push_map_as_event_on_timeout => true`에 의해 통계 이벤트로 발행되고 `timeout_tags => ['aggregated']`가 부착된다. |
| 6 | **상태 소멸** | `aggregate_maps_path` 미설정 시, Logstash 종료 시점의 잔여 맵은 소실된다. |

### 8.2 구조적 한계

| # | 한계 | 근거 / 본 벤치마크에서의 관측 |
|:---:|---|---|
| **1** | **워커 병렬화 불가** | Elastic 공식 문서는 aggregate 필터에 대해 **filter workers를 반드시 1로 설정(`-w 1`)** 할 것을 요구한다. 그렇지 않으면 이벤트가 순서를 벗어나 처리되어 예상치 못한 결과가 발생한다고 명시한다. **본 보고서 7.2절이 이를 정량적으로 재현했다.** |
| **2** | **처리량 상한** | 맵이 mutex로 보호되는 파이프라인 공유 자원이므로, 워커를 늘려도 aggregate 구간은 사실상 직렬 실행된다. **6.3절의 워커당 CPU 활용률 94.4% → 44.5% 붕괴가 직접 증거다.** |
| **3** | **드레인 고정 오버헤드** | 타임아웃 만료는 후속 이벤트 유입 또는 주기적 flush 시점에 평가된다. 스트림 종료 후 잔여 맵이 flush되기까지 timeout(20초) + flush 주기만큼의 지연이 불가피하다. **6.2절의 20~30초 드레인이 이에 해당하며, 유입이 빨라질수록 E2E의 33%까지 잠식한다.** |
| **4** | **수평 확장 불가** | 맵이 단일 JVM 힙에 존재한다. 다중 Logstash 인스턴스로 스케일아웃 시 동일 key가 서로 다른 노드로 분산되면 **병합 자체가 실패**한다. |
| **5** | **메모리 압박** | 동시 활성 key 수에 비례해 힙을 점유한다. 일일 수억 건 규모에서 key cardinality가 폭증하면 OOM 위험이 있다. 본 벤치마크의 Heap 여유(최대 64.5%)는 데이터셋의 제한된 cardinality에 기인하며, 실 운영으로 일반화할 수 없다. |
| **6** | **상태 유실 위험** | 재시작·장애 시 잔여 맵이 소실된다. `aggregate_maps_path`로 완화 가능하나, 파이프라인당 aggregate 필터 1개로 제한되고 파일 기반 직렬화라 대규모 맵에서 부담이 크다. |
| **7** | **워터마크 부재** | Flink/Spark의 watermark처럼 **지연 이벤트(late event)를 정의하고 처리하는 개념이 없다.** out-of-order 입력에 대한 정합성 보장 수단이 원천적으로 존재하지 않는다. 7.2절의 왜곡은 이 부재의 직접적 결과다. |

> **참고:** Elastic 공식 문서 — Aggregate filter plugin
> https://www.elastic.co/docs/reference/logstash/plugins/plugins-filters-aggregate

---

## 9. Logstash 대체 ETL 서비스 검토 제안서

### 9.1 검토 배경

본 벤치마크는 Logstash `aggregate` 필터의 세 가지 구조적 제약을 실증하였다.

1. **단일 노드 처리량 상한** — `pipeline.workers=1`이 정합성의 전제 조건이므로, 단일 인스턴스의 처리량은 4,762 EPS에 고정된다.
2. **수평 확장 불가** — 상태(맵)가 로컬 힙에 존재하여 스케일아웃이 원천 봉쇄된다.
3. **정합성 보장 수단 부재** — out-of-order 입력에 대한 watermark 개념이 없다.

일일 **수억 건** 규모의 방화벽 로그를 20초 윈도우로 병합하려면, 산술적으로 다음이 요구된다.

```
2억 건/일 ÷ 86,400초 ≈ 2,315 EPS (평균)
피크 배수 5배 가정      ≈ 11,600 EPS (피크)

Logstash 단일 인스턴스(W1) 처리량 = 4,762 EPS
→ 피크 대응에 최소 3대 이상 필요하나, 키가 분산되면 병합 실패
→ 키 기반 샤딩을 지원하는 분산 스트리밍 아키텍처가 필수
```

### 9.2 평가 기준

| 기준 | 가중치 | 설명 |
|---|:---:|---|
| **Event-time 윈도잉 정확성** | ★★★ | 20초 tumbling window + late event 처리 |
| **키 affinity 보장 수평 확장** | ★★★ | 3-tuple 키 기준 파티셔닝 후 병렬 처리 |
| **상태 관리 / 장애 복구** | ★★★ | checkpoint, exactly-once |
| 운영 성숙도 / 폐쇄망 구축 | ★★ | 금융권 망분리 환경 필수 |
| 자원 효율 | ★★ | 처리량 대비 CPU/Memory |
| 라이선스 / 도입 비용 | ★★ | |

### 9.3 후보 비교

| 후보 | Event-time 윈도잉 | 수평 확장 | 상태/복구 | 자원 효율 | 운영 성숙도 | 종합 |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Kafka + Flink** | ◎ watermark 기반 tumbling window | ◎ 키 파티셔닝 + operator 병렬도 | ◎ checkpoint / exactly-once | ○ | ○ | **1순위** |
| Kafka + Kafka Streams | ◎ | ◎ | ◎ | ○ | ○ | 2순위 |
| Kafka + Spark Structured Streaming | ○ watermark 지원, micro-batch | ◎ | ◎ | △ (배치 지연) | ◎ | 3순위 |
| **Vector** (Rust) | △ aggregate transform, event-time 약함 | ○ | △ | ◎ | ○ | 단기 대체 |
| Fluent Bit | △ | ○ | △ | ◎ | ◎ | 수집단 한정 |
| Apache NiFi | △ | ○ | ○ | △ | ○ | 부적합 |

### 9.4 제안 아키텍처 — Kafka + Flink

```
  Firewall
     │
     ▼
  Filebeat / Fluent Bit          ← 수집 (경량, 저자원)
     │
     ▼
  ┌──────────────────────────────────────────────────┐
  │  Kafka                                           │
  │  partition key = hash(src_ip, dst_ip, dst_port)  │
  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐     │
  │  │  P0    │ │  P1    │ │  P2    │ │  P3    │ ... │
  │  └────────┘ └────────┘ └────────┘ └────────┘     │
  └──────────────────────────────────────────────────┘
     │            │            │            │
     ▼            ▼            ▼            ▼
  ┌──────────────────────────────────────────────────┐
  │  Flink                                           │
  │    .keyBy(src_ip, dst_ip, dst_port)              │
  │    .window(TumblingEventTimeWindows.of(20s))     │
  │    .allowedLateness(5s)                          │
  │    .aggregate(SumAggregator)                     │
  │  → checkpoint 기반 exactly-once                  │
  └──────────────────────────────────────────────────┘
     │
     ▼
  Elasticsearch / 2차 시스템
```

**핵심 원리**

> **3-tuple을 Kafka 파티션 키로 사용하면, 동일 키의 이벤트는 항상 동일 파티션에 순서대로 유입된다.** 각 Flink operator는 자신에게 할당된 키 집합만 처리하므로, **Logstash가 워커 1개로만 보장하던 순서 정합성을 유지하면서 파티션 수만큼 선형 스케일아웃이 가능**하다.
>
> 즉 본 벤치마크에서 확인된 **"속도 ↔ 정합성" 트레이드오프가 구조적으로 해소된다.**

추가로 Flink의 **watermark**는 지연 이벤트를 명시적으로 정의·처리하므로, 8.2절 한계 #7이 해결된다. `allowedLateness`로 늦게 도착한 이벤트를 윈도우에 반영하거나 side output으로 분리할 수 있어, Logstash의 "기준 시계 역행" 문제가 발생하지 않는다.

### 9.5 단계별 전환 로드맵

| 단계 | 조치 | 기대 효과 | 리스크 |
|:---:|---|---|---|
| **1 (즉시)** | Logstash aggregate 파이프라인을 `pipeline.workers=1`로 고정 | 데이터 정합성 확보 | 처리량 4,762 EPS 상한 |
| **2 (단기)** | 3-tuple 해시 기준으로 N개 파이프라인/인스턴스 분할, 각각 `-w 1` 운영 | Logstash를 유지한 채 N배 확장 | 샤딩 로직 운영 복잡도, 리밸런싱 부재 |
| **3 (중기)** | Kafka 도입, Logstash를 Kafka consumer로 전환 (`kafka` input) | 버퍼링·재처리·백프레셔 확보, 장애 시 유실 방지 | 인프라 증설, 운영 인력 |
| **4 (장기)** | 집계 로직을 Flink로 이관, Logstash는 수집/라우팅만 담당 | exactly-once + 선형 확장 + late event 처리 | Flink 운영 역량 확보 필요 |

**단기 대안 — Vector:** Kafka 도입 전 과도기에는 Vector가 현실적 선택지다. Rust 기반으로 Logstash 대비 CPU·메모리 효율이 크게 우수하며, `aggregate` transform을 제공한다. 다만 event-time 윈도잉 의미론이 Flink 수준에 미치지 못하므로, **정합성이 절대적인 구간에는 부적합**하다.

---

## 10. 결론 및 종합 의견

### 10.1 Worker 증설의 효과와 한계

Worker 1 → 4에서 유입 처리 시간이 **210초 → 70초(3.00배)**, E2E가 **240초 → 90초(2.67배, 62.5% 단축)** 로 개선되었다. 그러나 Worker 4 → 8에서는 유입 처리가 **70초 → 60초(3.50배)** 로 소폭 개선되었을 뿐, **E2E는 90초로 전혀 개선되지 않았다.**

원인은 다음 두 가지이며, **하드웨어 포화가 아니다.**

| 자원 | Worker 8 실측 | 할당 한도 | 포화 여부 |
|---|---:|---:|:---:|
| CPU | 3.56 core (평균) / 4.56 core (최대) | **8 core** (cpuset 0-7) | ❌ **44.5 %** |
| Disk Read | 12.80 MB/s | NVMe M.2 (수천 MB/s) | ❌ **1% 미만** |
| Memory | 2,020.8 MB | 8 GB | ❌ 24.7 % |
| JVM Heap | 1,279.6 MB | 2 GB (`-Xmx2g`) | ❌ 62.5 % |
| Elasticsearch CPU | 0.75 core | 1.5 core | ❌ 50 % |

1. **`aggregate` 필터의 맵 접근 직렬화** — 워커당 CPU 활용률이 94.4% → 63.8% → 44.5%로 붕괴했다. Worker를 8배 늘렸으나 CPU 사용량은 3.8배만 증가했고, Worker 8은 Worker 4 대비 CPU를 **19.8% 더 소모하고 E2E는 0% 개선**했다.
2. **20초 타임아웃 드레인의 고정 오버헤드** — 유입 종료 후 잔여 맵 flush에 20~30초가 소요되며, 이는 Worker 수와 무관하다. E2E에서 차지하는 비중이 12.5% → 22.2% → **33.3%** 로 증가하여, 유입 가속분을 상쇄한다.

**따라서 본 파이프라인 구조에서 Worker 증설의 한계 효용은 4를 넘어서면 사실상 0이며, CPU 낭비만 발생한다.**

### 10.2 Aggregate 필터의 효율화 효과

20초 윈도우 통계 병합은 원본 999,999건을 **552,904 ~ 535,752건으로 44.7~46.4% 축소**시켰다. 일일 2억 건 규모에서 이는 **약 9,000만 건/일의 인덱싱 및 스토리지 절감**에 해당한다. 동시에 `sum(event_count)`이 999,999로 3케이스 전부 일치하여 **통계적 총량이 완전히 보존**됨을 검증하였다.

### 10.3 최종 제언

> **`aggregate` 필터를 사용하는 시간 기반 통계 파이프라인에서 `pipeline.workers=1`은 성능 옵션이 아니라 정합성의 전제 조건이다.**

Elastic 공식 문서가 filter worker를 1로 고정할 것을 요구하고 있으며, 본 벤치마크는 그 근거를 정량적으로 재현했다. Worker 8 환경에서 소규모 세션 26,316개가 소멸하고 총 윈도우 수가 17,152건 왜곡되었다. 이는 보안 탐지 룰의 임계치를 직접 훼손하는 수준이다.

| 요구 조건 | 권장 구성 |
|---|---|
| **데이터 정합성이 필수** (금융권 방화벽 등) | **`pipeline.workers=1` 고정.** 타협 불가. |
| **처리량 확보가 필요** | Worker 증설이 **아니라** 3-tuple 키 기반 샤딩을 통한 **수평 확장**. 단기적으로는 키 해시 기준 N개 파이프라인 분할(각 `-w 1`), 중장기적으로는 **Kafka(파티션 키 = src_ip + dst_ip + dst_port) + Flink(event-time tumbling window 20s + watermark)** 전환. |
| **정합성보다 속도가 우선하는 보조 파이프라인** | Worker 4까지 허용 가능. 단, 윈도우 밀도 분포에 0.4% 수준의 왜곡이 발생함을 인지해야 한다. |

**핵심은 "Worker를 몇 개로 할 것인가"가 아니라 "aggregate 상태를 어디에 둘 것인가"이다.** 상태가 단일 JVM 힙에 있는 한 워커 수는 정합성과 교환되는 자원일 뿐이며, 상태를 파티션 단위로 분리하는 순간 속도와 정합성을 동시에 얻을 수 있다.

---

## 부록 A. 주요 Config

전체 설정 파일은 `1_pipeline/`, `2_elk_config/` 디렉토리를 참조.

### A.1 docker-compose.yml (자원 격리 핵심)

```yaml
logstash:
  image: docker.elastic.co/logstash/logstash:8.11.0
  environment:
    - LS_JAVA_OPTS=-Xms2g -Xmx2g
    - xpack.management.enabled=true
    - xpack.management.pipeline.id=firewall_agg
  cpuset: "0-7"                 # 8개 논리 스레드 전용 할당
  deploy:
    resources:
      limits:   { cpus: '8.0', memory: 8G }
      reservations: { cpus: '8.0', memory: 8G }

elasticsearch:
  environment:
    - "ES_JAVA_OPTS=-Xms2g -Xmx2g"
  cpuset: "8-9"                 # Logstash와 코어 분리
  deploy:
    resources:
      limits: { cpus: '1.5', memory: 4G }
```

### A.2 metricbeat modules.d

```yaml
# docker.yml
- module: docker
  metricsets: [container, cpu, diskio, memory]
  period: 5s
  hosts: ["unix:///var/run/docker.sock"]

# logstash.yml
- module: logstash
  metricsets: [node, node_stats]
  period: 5s
  hosts: ["http://logstash:9600"]
```

### A.3 logstash.yml

```yaml
http.host: 0.0.0.0
http.port: 9600
xpack.management.enabled: true
xpack.management.pipeline.id: firewall_agg
xpack.management.elasticsearch.hosts: http://elasticsearch:9200
xpack.monitoring.enabled: false     # Metricbeat 수집과의 이중 계상 방지
```

### A.4 Worker 설정 변경 방법

Centralized Pipeline Management를 사용하므로, Kibana → Stack Management → Logstash Pipelines에서 `firewall_agg` 파이프라인의 `pipeline.workers` 값을 1 / 4 / 8로 변경한 후 재시작하여 각 테스트를 수행하였다.
