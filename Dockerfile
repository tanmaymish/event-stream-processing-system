# Stage 1: Build
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY .mvn ./.mvn
COPY pom.xml .
RUN mvn dependency:go-offline -s .mvn/settings.xml -q 2>/dev/null || true
COPY src ./src
RUN mvn clean package -Dmaven.test.skip=true -Dcheckstyle.skip=true -q -s .mvn/settings.xml

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
RUN addgroup -S upi && adduser -S upi -G upi
COPY --from=builder /build/target/kafka-streams-examples-*-standalone.jar /app/upi-streaming.jar
USER upi
EXPOSE 8090 8091 8092 8093
# Default: runs UpiTransactionService; override entrypoint in docker-compose per service
ENTRYPOINT ["java", "-Xms256m", "-Xmx512m", "-cp", "/app/upi-streaming.jar", "io.confluent.examples.streams.microservices.UpiTransactionService"]
CMD ["kafka:29092", "http://schema-registry:8081", "8090"]
