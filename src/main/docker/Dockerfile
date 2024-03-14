FROM  eclipse-temurin:8-jdk-focal
ENV HOME /app
WORKDIR /app
ADD target/db-to-avro-1.6-SNAPSHOT.jar /app/db-to-avro.jar
ADD conf/app-prod.properties /app/conf/app.properties

ENTRYPOINT ["java","-Djava.security.egd=file:/dev/../dev/urandom","-Djava.awt.headless=true","-Duser.timezone=America/Los_Angeles","-jar","/app/db-to-avro.jar"]
