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
COPY test.ttl .

EXPOSE 8000
CMD ["java", "-jar", "app.jar"]
