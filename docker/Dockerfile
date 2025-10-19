FROM maven:3.9.11-eclipse-temurin-25 AS builder
WORKDIR /workspace

COPY pom.xml ./
RUN mvn -B -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -B -DskipTests package

FROM trinodb/trino:476
ARG PLUGIN_NAME=s3_file
COPY --from=builder /workspace/target/s3-file-plugin-0.1.0-SNAPSHOT /usr/lib/trino/plugin/${PLUGIN_NAME}
COPY docker/etc /etc/trino
