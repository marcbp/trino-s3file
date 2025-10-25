FROM maven:3.9.11-eclipse-temurin-25 AS builder
WORKDIR /workspace

COPY pom.xml ./
RUN mvn -B -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -B -DskipTests package && ls -l target/

FROM trinodb/trino:476
COPY --from=builder /workspace/target/trino-s3file /usr/lib/trino/plugin/s3file
COPY docker/etc /etc/trino
