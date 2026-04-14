FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
COPY batch-service/pom.xml batch-service/pom.xml
COPY batch-service/src batch-service/src

RUN mvn -pl batch-service -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /workspace/batch-service/target/batch-service-*.jar app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
