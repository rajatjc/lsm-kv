FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
# Download Gradle in its own layer so code changes don't re-fetch it.
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
RUN ./gradlew --no-daemon --version
COPY src src
RUN ./gradlew --no-daemon jar

# Runs as root so it can write to platform-mounted volumes (Fly/Render mount them root-owned).
FROM eclipse-temurin:21-jre
COPY --from=build /src/build/libs/lsm-kv.jar /app/lsm-kv.jar
ENV DATA_DIR=/data PORT=8080
EXPOSE 8080
# Serial GC and a heap capped at 70% of memory suit a small VM.
ENTRYPOINT ["java", "-XX:+UseSerialGC", "-XX:MaxRAMPercentage=70", "-jar", "/app/lsm-kv.jar"]
