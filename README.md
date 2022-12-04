# 332project

## Week1 

### Progress of the week

- create repo
- take midterm

## Week2 

### Goal of the Week 

- gensort input data generation 공부
- gRPC 통신 공부
- sorting 알고리즘 이해, 대략적 구조 설계

### Goal of the individual member

빙준호

- sorting 알고리즘 이해, 대략적 구조 설계
- gensort input data generation 공부

최규민

- gRPC 통신 공부
- gensort input data generation 공부

김지훈

- setup scala environment
- sorting 알고리즘 이해, 대략적 구조 설계

## Week 3

### Progress of Week 2

- 개발 환경 통일
- gRPC 공부, 알고리즘 대략적 이해

### general goals

- 커밋 메시지 형식 통일, coding style guide
- disk-based merge sort library 찾기

- master, slave 각각 할 일 전체적인 draft → milestone 짜기
- message 형식, 언제, 어떻게 보낼건지
- master slave network connection 메시지 띄우기 (gRPC)

### individual goals

빙준호

- gRPC 공부
- master, slave 각각 할 일 전체적인 draft → milestone 짜기
- message 형식, 언제, 어떻게 보낼건지

최규민

- master slave network connection 메시지 띄우기 (gRPC)

김지훈

- gRPC 공부
- disk-based merge sort library 찾기
- 데이터 교환 방식 정하기

## Week 4

### Progress of Week 3

- 전체 flow, 메시지 디자인
- master-slave connection
- disk based merge sort library 확인

### General Goals

- sampling 방법 - disk에서 읽는 방법
- 데이터 전송 방법
- Milestone3- Sort : sorting high order function 작성

### Individual Goals

빙준호

- Milestone 2 : Sampling
- sampling 방법 - disk에서 읽는 방법

최규민

- Milestone 1 : Init
- file transfer 방법

김지훈

- Milestone3 : Sort
- sorting high order function 작성

## Week 5

### Progress of Week 4

- Milestone 1: Init 완료
- Sorting function 구현
- Progress Presentation

### General Goals
Milestone 2, 3, 4

### Individual Goals

빙준호

- Milestone 2: Sampling
- Sampling 방법 - disk에서 읽는 방법

최규민

- 서버에서 master/worker 통신
- file transfer
- Milestone 6: Merge

김지훈
- Milestone 3: Sort
- Milestone 4: Partition
- Milestone 6: Merge

## Week 6

### Progress of Week 5

- 각 모듈 input, output 명세 및 사용방식 작성
- external mergesort 외부소스코드를 현재 프로젝트에 사용 가능하도록 기능 분할 및 수정
- Milestone 3 Sort : version error 수정 및 완성, edge case test
- Milestone 4 Partition : sampling 및 shuffling 과의 연계 방식 지정 및 구현
- Milestone 6 Merge : 소스코드 분석 및 편집 계획 수립.

### Individual Goals

빙준호
- Milestone 2: Sampling

최규민
- file transfer
- Milestone 6: Merge

김지훈
- Milestone 3: Sort
- Milestone 4: Partition
- Milestone 6: Merge

## Week 7

### Progress of Week 6
- Milestone 3 Sort
- Milestone 6 Merge

### Individual Goals

빙준호
- Milestone 2: Sampling
- data transfer

최규민
- data transfer

김지훈
- Milestone 4: Partition

## Week 8

### Progress of Week 7
- Data transfer
- Sampling 방법 재논의, 각 worker가 sample data와 'data 개수 / chunck size'도 master에게 전달하고, 이를 리스트로 만들어 각 worker에게 전달하기로 함.
- milestone 5: shuffle에서 필요한 data 논의

### Individual Goals

빙준호
- Milestone 2: Sampling
- main, 앞뒤 milestone 과의 syncronization

최규민
- Milestone 5: Shuffle
- main, 앞뒤 milestone 과의 syncronization

김지훈
- Mileston 4: Partition
- main, 앞뒤 milestone 과의 syncronization
