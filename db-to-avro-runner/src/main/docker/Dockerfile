FROM openjdk:8
ENV HOME /app
WORKDIR /app
ADD target/db-to-avro-runner-1.0-SNAPSHOT.jar /app/db-to-avro.jar
ADD conf/app.properties /app/conf/app.properties 

ENTRYPOINT ["java","-Djava.awt.headless=true","-jar","/app/db-to-avro.jar"]
