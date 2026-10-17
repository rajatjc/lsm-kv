plugins {
    // Downloads a matching JDK when the toolchain below isn't installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "lsm-kv"
