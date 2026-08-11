# syntax=docker/dockerfile:1

FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 \
    mvn --batch-mode --no-transfer-progress package -DskipTests

FROM eclipse-temurin:21-jre-jammy

RUN groupadd --system --gid 10001 raftkv \
    && useradd --system --uid 10001 --gid raftkv --home-dir /app --shell /usr/sbin/nologin raftkv \
    && mkdir -p /app /data \
    && chown -R raftkv:raftkv /app /data

WORKDIR /app
COPY --from=build --chown=raftkv:raftkv /workspace/target/raftkv.jar /app/raftkv.jar

USER raftkv
VOLUME ["/data"]
EXPOSE 6379 7000

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/raftkv.jar"]
