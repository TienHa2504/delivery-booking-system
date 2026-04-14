FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml .
COPY auth-service/pom.xml auth-service/pom.xml
COPY booking-service/pom.xml booking-service/pom.xml
COPY batch-service/pom.xml batch-service/pom.xml
COPY notification-service/pom.xml notification-service/pom.xml
COPY booking-service/src booking-service/src

RUN mvn -pl booking-service -am package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /workspace/booking-service/target/booking-service-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
