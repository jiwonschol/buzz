# 사용량 보류 후속 리뷰

대상은 [Buzz #2](https://github.com/jiwonschol/buzz/pull/2)의
`e32444fbaa9ebd9d640b6a77e94baba5c192f5e5` 이후 수정이다.
원 PR #1의 브랜치와 운영 실행 파일은 변경하지 않는다.

## 리뷰별 처리

| 리뷰 / 인라인 ID | 처리와 검증 |
| --- | --- |
| 5289771920 / 4081434810 | 미실행 배치를 반환할 때 `release_in_flight`로 소유권만 해제한다. 실제 `dispatch_pending`의 작업자 부족 경로와 보류 후 용량 초과 반례로 보호·횟수 유지 확인. |
| 5289771920 / 4081434815 | 15분 지난 서명은 채널과 기존 ID를 조회한다. 접수된 ID는 완료 처리하고, 조회 성공·미발견일 때만 새 timestamp로 서명해 먼저 저장한다. 조회 오류는 미발견으로 취급하지 않는다. 로컬 HTTP fixture로 세 경로 확인. |
| 5289771920 / 4081434827 | 보류 통지를 main loop에서 동기 저장한 뒤 별도 worker가 전송한다. 저장 실패는 오류를 기록하되 요청 큐와 실행 루프를 유지한다. 재시작은 저장된 레코드를 읽어 같은 ID로 재시도한다. 실패·거절·성공 응답과 디스크 복구 경로를 확인. |
| 5289771920 / 4081434834 | `flush_next`와 `has_flushable_work` 모두 in-flight 만료 복구를 계정 보류 검사보다 먼저 실행한다. 두 실제 진입점에서 만료 회귀 확인. |
| 5289771920 / 4081434839 | heartbeat dispatch에도 계정 보류를 검사한다. 실제 dispatch 호출에서 작업자를 점유하지 않는지 확인. |
| 5290160297 / 4081748476 | listener의 steer/interrupt 공통 진입점에서 계정 보류를 검사한다. 두 처리 모드에서 실제 control 채널에 취소 신호가 나가지 않는지 확인. |
| 5290160297 / 4081748483 | 첫 보류의 절대 8일 deadline을 저장하고 이후 대기 시간을 그 기한으로 자른다. 기한 후 재보류는 terminal 처리한다. 90초·30분·8일 지연 모두 같은 경계 확인. |
| 5290160297 / 4081748488 | 명시적 usage/session/weekly limit은 rate-limit wrapper보다 우선한다. 일반 rate-limit 배제와 명시적 고갈 판정 모두 확인. |
| 5290160297 / 4081748496 | 채널 cap 적용 후 삽입한 ID가 남아 있는 경우만 `push`가 성공을 반환한다. 10개 thread의 50개 보류 요청으로 500개 cap을 채우고 새 요청 거절 확인. |
| e2af578 리뷰 / 4113576344 | 통지 저장 실패만으로 루프를 종료하지 않는다. 저장 경로가 일반 파일인 반례에서 요청 보존·in-flight 해제·재시도 deadline과 Continue를 확인한다. |
| e2af578 리뷰 / 4113576350 | 큐의 다음 retry deadline을 select 타이머에 연결한다. 계정 deadline을 함께 적용하고 깨어난 타이머의 만료값만 소비해 busy spin을 막는다. 주기 기능 없이 같은 대기 함수로 깨어나 배치를 꺼내는 회귀를 추가했다. |
| e2af578 리뷰 / 4113576355 | 괄호 밖 UTC/GMT/IANA/숫자 offset도 인식해 로컬 시각으로 오해하지 않고 기존 30분 fallback을 쓴다. |
| e2af578 리뷰 / 4113576357 | 신규 접수 시 실행 중 배치의 복귀 용량을 예약한다. 500개가 실행 중일 때 11번째 배치의 신규 요청을 거절하고, 모두 보류 복귀해도 500개를 유지한다. |

## 통지 저장 계약

후속 4ee6e951 리뷰 5328420805의 다섯 건도 반영했다.

| 인라인 ID | 처리 |
| --- | --- |
| 4113682881 | 동기 저장 실패 시 최대 1,024개의 bounded retry 작업으로 5초~5분 간격의 독립 저장 재시도를 시작한다. 최초 시각 기준 8일 한도를 유지한다. 추가 usage 결과 없이 저장 경로 복구 후 파일 생성 회귀 확인. |
| 4113682882 | 요청 상세는 최대 10개와 생략 개수로 제한한다. agent/turn 문자열도 제한하며 thread 링크는 유효한 ID만 포함한다. 500개 다중바이트 요청의 signed notice가 16 KiB 미만인지 확인. |
| 4113682884 | batch 유지 여부나 채널 제거와 무관하게 모든 usage-limit 결과에 먼저 account hold를 적용한다. 제거된 채널 결과 뒤 다른 요청 dispatch가 막히는 회귀 확인. |
| 4113682886 | 취소 결과에서 실제 batch를 보존한 경우만 소유권을 해제한다. batch 없는 명시적 취소는 이전 retry/hold/notice 메타데이터를 정리한다. |
| 4113682887 | retry timer를 pool_ready와 무관하게 활성화한다. 잠든 lazy pool에서는 만료값 소비 후 루프 처음의 wake 경로로 돌아간다. |

후속 cfeb496 리뷰 5328371745도 반영했다.

| 인라인 ID | 처리 |
| --- | --- |
| 4113637058 | 최초 보류 횟수와 통지 저장/재시도 접수를 분리했다. 후속 4113682881에서 현재 보류 중에도 독립 저장 재시도가 돌도록 보완했다. |
| 4113637063 | batch 없는 heartbeat 결과도 usage-limit을 분류하고 계정 hold를 설정한다. 실제 결과 처리 후 다음 heartbeat dispatch가 작업자를 점유하지 않는지 확인. |
| 4113637068 | cancelled/withheld를 채널 대기 수에 포함하고 merged batch의 실행 예약에 cancelled 수를 더한다. 일반 오류 복귀도 cancelled carryover를 복원한다. |
| 4113637071 | `resets 3:10am UTC.`와 `resets 15:30 GMT;`, 괄호·offset 문장부호를 회귀에 포함하고 토큰 양끝 문장부호를 정규화한다. |
| 4113637072 | 저장/삭제 성공과 독립적인 process-local cursor로 outbox를 순환한다. 33개 기록의 삭제를 모두 실패시켜도 두 pass에서 전부 전송되는 HTTP 회귀를 추가했다. HOME 경로 조회 실패도 worker를 종료하지 않고 재시도한다. |

요청 상태별 대조는 다음과 같다. 일반 대기 큐의 기존 oldest-eviction 정책은 유지하고,
실행 중 복귀분·보류 보호분·취소 carryover에는 신규 접수가 그 용량을 침범하지 못하게 한다.

| 전이 | 용량·복구 처리 |
| --- | --- |
| queued → in-flight | 큐에서 빠진 events와 cancelled 전체 수를 실행 예약으로 옮긴다. |
| in-flight → cancelled → merged | 기존 요청을 cancelled에 보관해 채널 총량에 포함한다. 재병합 시 실행 예약에 포함한다. |
| in-flight → held / 일반 오류 / 작업자 대기 | 공통 복귀 함수로 events·cancelled·원래 시각을 복원한다. hold는 양쪽 ID를 보호한다. |
| queued → withheld → released / consumed | withheld도 대기 총량에 포함한다. release는 같은 수를 큐로 옮기고 성공 확인 후에만 제거한다. |
| 취소·작업자 대기 / 실제 완료 | 전자는 소유권만 해제해 hold 메타데이터를 보존한다. 후자는 기존 완료 정책으로 정리한다. |

반복 취소로 500개 요청을 누적한 뒤 네 복귀 경로(held/retry/preserve/cancelled)를
각각 검증한다. maintenance가 아직 소비되지 않은 retry wake-up을 지우지 않는 것도 확인한다.

저장 위치는 `$HOME/.buzz/notice-outbox/<relay URL + agent pubkey의 SHA-256>/`다.
새 외부 의존성이나 relay 스키마 변경 없이 기존 HTTP submit/query 경로를 사용한다.
파일당 64 KiB, 디렉터리당 1,024개로 제한한다. 서명 이벤트를 임시 파일에 쓰고
fsync·rename한 뒤 전송하며 Unix 파일 권한은 0600이다. 5초부터 최대 5분까지
backoff하고 한 pass는 최대 32건을 순환 처리한다.
8일 만료·손상·전송 실패 레코드는 삭제하지 않는다. 접수 성공이 확인된 기록만 지운다.
한도가 차면 새 저장은 오류를 반환하며 운영자가 보존 레코드를 확인해야 한다.
저장 자체가 실패한 통지는 독립 타이머가 다시 저장을 시도한다. 최초 저장 전에는
메모리뿐이므로 그 전에 프로세스가 종료되면 복구를 보장하지 않는다. 요청 루프는 유지한다.
메모리 저장 재시도도 1,024개/최초 8일로 제한하며 한도 초과는 명시적 오류를 남긴다.

요청 큐 자체는 여전히 메모리 상태다. 통지 복구는 요청의 재시작 복구를 뜻하지 않는다.
따라서 통지 본문은 보류 발생 시각과 당시 상태를 설명하고, harness 재시작 시 재전송이
필요하다는 한계를 명시한다. 통지가 늦게 도착해도 완료나 현재 대기를 단정하지 않는다.
별도 harness들이 같은 provider 계정을 쓰는 경우의 전역 조율은 범위 밖이다.

## 검증 기록

9ed5b469의 후속 리뷰 두 건도 반영한다. 4113730782는 `std::env::home_dir`로
Windows 프로필과 Unix 계정 홈 fallback을 사용한다. 4113730784는 저장소에서 이미
사용하는 fs2 0.4.3의 OS 파일 잠금으로 용량 확인부터 저장까지 직렬화한다.
잠금 충돌은 기존 저장 재시도로 돌아가며, 프로세스 종료 시 OS가 잠금을 해제한다.
영구 `.admission.lock` 하나는 레코드 수에서 제외하며 삭제하지 않는다.
16개 동시 쓰기가 남은 한 자리에 입장하는 회귀로 1,024 레코드 상한을 검증한다.
표준 File 잠금은 Rust 1.89부터라 현재 MSRV 1.88에서 쓸 수 없다.
근거: https://doc.rust-lang.org/std/env/fn.home_dir.html 및
https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock.

9ed5b469 전체 CI는 기존 Pi 실행기 시험의 `Text file busy`로 exit 1이었다.
같은 SHA의 ACP/core 전체 재시험은 1,250 PASS·exit 0이었다.
후속 단계 실행은 새 리뷰 수정으로 중단했으며 전체 CI PASS로 취급하지 않는다.

4ee6e951 이후 다섯 건 수정 트리의 전체 시험은 ACP 981 + integration 9 + core 258 +
doctest 2, 총 1,250 PASS다. 로그는 `buzz-review-round4-tests.log`다.
unused test 변수 경고는 이후 이름 수정으로 제거했다. 최종 SHA 검사는 PR 본문에 확정한다.

cfeb496 이후 다섯 건과 전이 경계 수정 트리의 전체 시험은 ACP 976 + integration 9 +
core 258 + doctest 2, 총 1,245 PASS다. 로그는 `buzz-review-round3-final-tests.log`다.
cfeb496 전체 CI는 후속 수정으로 중단했고, 새 커밋의 전체 CI·리뷰를 별도로 확인한다.

e2af578 이후 추가 네 건의 수정 트리에서 전체 ACP 971 + integration 9 + core 258 +
doctest 2, 총 1,240개가 통과했다. 용량 예약과 괄호 밖 시간대 회귀는 수정 전
967 PASS / 2 FAIL을 확인했다. 저장 실패는 고유 outbox 경로를 일반 파일로 막아
실제 파일 저장 오류를 발생시켰고, Continue·요청 보존·in-flight 해제·retry deadline을 확인했다.
로그는 `buzz-review-round2-red.log`, `buzz-review-round2-green-final.log`다.
이전 후보의 전체 CI는 새 리뷰 수정 때문에 중단했으며 완료로 취급하지 않는다.
새 커밋의 전체 CI·자동 리뷰·독립 판정은 PR 본문에서 별도로 확정한다.

첫 수정본의 전체 `cargo test -p buzz-core -p buzz-acp --no-fail-fast`는
ACP 966 + integration 9 + core 258 + doctest 2, 총 1,235개 통과했다.
이후 outbox 저장 한도 테스트와 문구·테스트 파일 분리를 추가했으므로 최종 커밋 결과는
별도로 확정해야 한다. 전체 CI와 최종 SHA 자동 리뷰·독립 판정은 아직 진행 중이다.
로컬 로그: `/home/buzz-hyuncheol/.buzz/.scratch/buzz-review-20260927-tests.log`,
`buzz-review-20260927-ci.log`. 실제 provider·운영·UI 확인을 의미하지 않는다.
