resolvers ++= Seq(
  Resolver.mavenLocal,
  "confluent" at "http://packages.confluent.io/maven/")

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "kafka.streams",
      scalaVersion := "2.12.2",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "StreamConsumer",
    libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.10.2.1",
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.1",
    libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "1.7.0"
  )
