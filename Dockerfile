# syntax=docker/dockerfile:1.7
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml ./
COPY pg-scheduler-core/pom.xml pg-scheduler-core/
COPY pg-scheduler-server/pom.xml pg-scheduler-server/
COPY pg-scheduler-core/src pg-scheduler-core/src
COPY pg-scheduler-server/src pg-scheduler-server/src
# Tests need Docker (Testcontainers) and run in CI; the image build only packages.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -DskipTests -pl pg-scheduler-server -am package \
    && cp pg-scheduler-server/target/pg-scheduler-server-*.jar /build/app.jar

FROM eclipse-temurin:25-jre
# curl is only for the container health check.
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home scheduler
WORKDIR /app
COPY --from=build /build/app.jar /app/app.jar
USER scheduler
EXPOSE 8207
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
