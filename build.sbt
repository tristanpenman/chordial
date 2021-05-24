name := "chordial"

lazy val commonSettings = Seq(
  organization := "com.tristanpenman",
  version := "0.0.1",
  ThisBuild / scalafmtOnCompile := true,
  ThisBuild / scalafmtVersion := "1.4.0",
  ThisBuild / scalaVersion := "2.13.6",
  resolvers ++= Seq(
    "Typesafe Releases" at "https://repo.typesafe.com/typesafe/maven-releases/",
    Resolver.jcenterRepo,
    Resolver.sonatypeRepo("releases"),
    Resolver.bintrayRepo("akka", "maven")
  ),
  scalacOptions := Seq(
    "-feature",
    "-unchecked",
    "-deprecation",
    "-opt-warnings:_",
    "-unchecked",
    "-Xlint:_",
    "-Ywarn-dead-code",
    "-Ywarn-extra-implicit",
    "-Ywarn-unused:_"
  ),
  // Work-around for metaspace OOM issues that occur with Scala 2.12.8 and sbt 1.3.0-RC1
  Test / fork := true,
  Test / javaOptions ++= Seq("-Xmx256m")
)

lazy val akkaVersion = "2.5.32"
lazy val akkaHttpVersion = "10.1.12"
lazy val guavaVersion = "29.0-jre"
lazy val scalatestVersion = "3.1.2"

lazy val core = project
  .in(file("modules/core"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )

lazy val demo = project
  .in(file("modules/demo"))
  .dependsOn(core)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "ch.megard" %% "akka-http-cors" % "0.4.3",
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )

lazy val dht = project
  .in(file("modules/dht"))
  .dependsOn(core)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "ch.megard" %% "akka-http-cors" % "0.4.3",
      "com.google.guava" % "guava" % guavaVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test",
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
