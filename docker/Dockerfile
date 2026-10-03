FROM maven:3.9.16-eclipse-temurin-25 AS builder
WORKDIR /workspace

COPY pom.xml ./
RUN mvn -B -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -B -DskipTests package && mkdir target/plugin && cd target/plugin && jar xf ../trino-s3file-*.zip

FROM trinodb/trino:483
COPY --from=builder /workspace/target/plugin/trino-s3file-* /usr/lib/trino/plugin/s3file/
COPY docker/etc /etc/trino
