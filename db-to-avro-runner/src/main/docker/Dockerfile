FROM openjdk:8
ENV HOME /app
WORKDIR /app
ADD db-to-avro-runner-1.0-SNAPSHOT.jar /app/db-to-avro.jar

ENTRYPOINT ["java","-Djava.awt.headless=true","-jar","/app/db-to-avro.jar"]
