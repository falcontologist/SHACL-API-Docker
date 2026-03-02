# Step 1: Build the JAR
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
COPY roles_shacl.ttl .
RUN mvn package -DskipTests

# Step 2: Fetch LFS-hosted TTL files
FROM alpine/git:latest AS lfs-fetch

# Install Git LFS (alpine/git doesn't include it by default)
RUN apk add --no-cache git-lfs \
    && git lfs install --system

WORKDIR /data

# Clone and fetch LFS files properly
RUN git clone https://github.com/falcontologist/SHACL-API-Docker.git . \
    && git lfs fetch --all \
    && git checkout main \
    && git lfs pull \
    && git checkout main -- \
       lexical.ttl \
       person_entity.ttl \
       person_entry.ttl \
       organization_entity.ttl \
       organization_entry.ttl \
       gpe_entity.ttl \
       gpe_entry.ttl \
       product_entity.ttl \
       product_entry.ttl

# Verify files are real TTL content
RUN head -1 /data/gpe_entity.ttl | grep -q "prefix" \
    && echo "=== LFS pull verified OK ===" \
    || (echo "ERROR: gpe_entity.ttl is still an LFS pointer!" && cat /data/gpe_entity.ttl && exit 1)

# Step 3: Run the JAR
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/shacl-service-1.0-SNAPSHOT-jar-with-dependencies.jar ./app.jar
COPY roles_shacl.ttl .
COPY test2.ttl .
COPY --from=lfs-fetch /data/lexical.ttl ./lexical.ttl
COPY --from=lfs-fetch /data/person_entity.ttl ./person_entity.ttl
COPY --from=lfs-fetch /data/person_entry.ttl ./person_entry.ttl
COPY --from=lfs-fetch /data/organization_entity.ttl ./organization_entity.ttl
COPY --from=lfs-fetch /data/organization_entry.ttl ./organization_entry.ttl
COPY --from=lfs-fetch /data/gpe_entity.ttl ./gpe_entity.ttl
COPY --from=lfs-fetch /data/gpe_entry.ttl ./gpe_entry.ttl
COPY --from=lfs-fetch /data/product_entity.ttl ./product_entity.ttl
COPY --from=lfs-fetch /data/product_entry.ttl ./product_entry.ttl
EXPOSE 8080
CMD ["java", "-jar", "app.jar"]