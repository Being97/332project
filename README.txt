README
• document how to build your system in the VM cluster
input 데이터는 각 worker의 vm의 프로젝트 디렉토리에서 ./src/main/resources/worker/input 에 저장되어야 한다. 파일명은 상관 없으나, 각 파일당 크기는 메모리에 올라갈 수 있을만큼 충분히 작아야 한다. 

/src/main/resources 의 하위 폴더는 폴더 구조만 유지하고 내부에 input을 제외한 다른 파일이 없어야 한다.

실행은 마스터에서 sbt "runMain master.Master <# of workers>"
워커에서 sbt "runMain worker.Worker <Master IP>"