# Kafka-mysql-consumer    
카프카에서 발생되는 로그에서 특정로그만 mysql로 저장    

Data-center에서 micro 인스턴스 사용을 위해 flask CRUD를 뺀 서버 입니다.       
카프카에서 데이터센터에 필요한 로그가 발생하면 Mysql에 데이터를 적재합니다.    
* Mysql서버 1개 - port 3306
* Kafka-Consumer 1개
