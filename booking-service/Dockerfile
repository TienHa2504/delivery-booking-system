FROM maven:3.9.9-eclipse-temurin-21 AS dependencies
WORKDIR /workspace

COPY pom.xml ./
COPY auth-service/pom.xml auth-service/pom.xml
COPY booking-service/pom.xml booking-service/pom.xml
COPY batch-service/pom.xml batch-service/pom.xml
COPY notification-service/pom.xml notification-service/pom.xml

RUN mvn -B -pl booking-service -am dependency:go-offline

FROM dependencies AS build
COPY booking-service/src booking-service/src

RUN mvn -B -pl booking-service -am package -DskipTests

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --home-dir /app app

COPY --from=build --chown=app:app /workspace/booking-service/target/booking-service-*.jar /app/app.jar

USER app
EXPOSE 8080

ENV JAVA_OPTS=""
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
