ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "iotanon_full",

    resolvers ++= Seq(
      "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
      "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
      "MavenRepository" at "https://mvnrepository.com",
      "Confluent" at "https://packages.confluent.io/maven/",
      "Akka" at "https://repo.akka.io/maven/"
    ),

    dependencyOverrides ++= Seq(
      "com.github.luben" % "zstd-jni" % "1.5.6-4",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
    ),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.4",
      "org.apache.spark" %% "spark-sql" % "3.5.4",
      "org.apache.spark" %% "spark-avro" % "3.5.4",
      "org.apache.kafka" %% "kafka-streams-scala" % "3.9.0",
      "io.prometheus" % "client" % "0.0.10",
      "io.prometheus" % "simpleclient" % "0.16.0",
      "io.prometheus" % "simpleclient_pushgateway" % "0.16.0",
      "io.prometheus" % "simpleclient_hotspot" % "0.16.0",
      "org.dmonix" %% "prometheus-client-scala" % "1.2.0",
      "io.confluent" % "kafka-avro-serializer" % "7.8.0",
      "com.typesafe" % "config" % "1.4.3",
    )
  )