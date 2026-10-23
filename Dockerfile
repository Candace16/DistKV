# Multi-stage Dockerfile for DistKV
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests

# Lean runtime stage
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=builder /app/target/distkv-*.jar app.jar
EXPOSE 7000 7001 7002
ENTRYPOINT ["java", "-cp", "app.jar", "com.anjali.distkv.Main"]
