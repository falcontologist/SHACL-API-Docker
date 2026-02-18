# Step 1: Build the JAR
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
COPY roles_shacl.ttl .
RUN mvn package -DskipTests

# Step 2: Run the JAR
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/shacl-service-1.0-SNAPSHOT-jar-with-dependencies.jar ./app.jar
COPY roles_shacl.ttl .
COPY test2.ttl .

# Cache lexical partition at build time — large file, changes infrequently
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -f -o /app/lexical.ttl https://raw.githubusercontent.com/falcontologist/SHACL-API-Docker/main/lexical.ttl \
    && apt-get purge -y curl && rm -rf /var/lib/apt/lists/*

EXPOSE 8080
CMD ["java", "-jar", "app.jar"]