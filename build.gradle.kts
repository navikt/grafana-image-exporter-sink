import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val slf4jVersion = "1.7.25"
val ktorVersion = "1.1.2"
val kafkaVersion = "2.0.1"
val arrowVersion = "0.9.0"
val prometheusVersion = "0.6.0"
val s3SdkVersion = "2.5.29"
val junitJupiterVersion = "5.4.0"
val mainClass = "no.nav.helse.AppKt"

plugins {
    kotlin("jvm") version "1.3.21"
}

buildscript {
    dependencies {
        classpath("org.junit.platform:junit-platform-gradle-plugin:1.2.0")
    }
}

dependencies {
    compile(kotlin("stdlib"))
    compile("ch.qos.logback:logback-classic:1.2.3")
    compile("net.logstash.logback:logstash-logback-encoder:5.2")
    compile("io.ktor:ktor-server-netty:$ktorVersion")

    compile("org.apache.kafka:kafka-streams:$kafkaVersion")

    compile("io.prometheus:simpleclient_common:$prometheusVersion")
    compile("io.prometheus:simpleclient_hotspot:$prometheusVersion")

    compile("software.amazon.awssdk:kinesis:$s3SdkVersion")

    compile("io.arrow-kt:arrow-core-data:$arrowVersion")

    testCompile ("no.nav:kafka-embedded-env:$kafkaVersion")

    testCompile("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testCompile("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")

    testCompile("io.ktor:ktor-server-test-host:$ktorVersion") {
        exclude(group = "junit")
    }
}

repositories {
    jcenter()
    mavenCentral()
    maven("https://dl.bintray.com/kotlin/ktor")
    maven("http://packages.confluent.io/maven/")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.named<Jar>("jar") {
    baseName = "app"

    manifest {
        attributes["Main-Class"] = mainClass
        attributes["Class-Path"] = configurations["compile"].joinToString(separator = " ") {
            it.name
        }
    }

    doLast {
        configurations["compile"].forEach {
            val file = File("$buildDir/libs/${it.name}")
            if (!file.exists())
                it.copyTo(file)
        }
    }
}

tasks.named<KotlinCompile>("compileKotlin") {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.named<KotlinCompile>("compileTestKotlin") {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.withType<Wrapper> {
    gradleVersion = "5.2.1"
}
