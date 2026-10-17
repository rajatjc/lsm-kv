plugins {
    java
    application
}

group = "io.github.rajatjc"
version = "0.2.0"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val mainClassName = "io.github.rajatjc.lsmkv.server.KvServer"

application {
    mainClass = mainClassName
}

// Benchmarks live in their own source set so they never ship in the server jar.
val jmh = sourceSets.create("jmh") {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.12.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    "jmhImplementation"("org.openjdk.jmh:jmh-core:1.37")
    "jmhAnnotationProcessor"("org.openjdk.jmh:jmh-generator-annprocess:1.37")
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    archiveFileName = "lsm-kv.jar"
    manifest {
        attributes("Main-Class" to mainClassName)
    }
}

tasks.register<JavaExec>("jmh") {
    description = "Runs the JMH benchmarks. Pass a filter with --args, e.g. --args=ReadBenchmark"
    group = "verification"
    classpath = jmh.runtimeClasspath
    mainClass = "org.openjdk.jmh.Main"
}
