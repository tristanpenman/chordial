name := "Chordial"

lazy val commonSettings = Seq(
  organization := "com.tristanpenman",
  version := "0.0.1",
  scalafmtOnCompile in ThisBuild := true,
  scalafmtVersion in ThisBuild := "1.4.0",
  scalaVersion := "2.12.8",
  resolvers ++= Seq(
    "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/",
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
    "-Xfuture",
    "-Xlint:_",
    "-Ywarn-dead-code",
    "-Ywarn-extra-implicit",
    "-Ywarn-inaccessible",
    "-Ywarn-infer-any",
    "-Ywarn-nullary-override",
    "-Ywarn-unused:_"
  )
)

lazy val akkaVersion = "2.5.17"
lazy val akkaHttpVersion = "10.1.4"
lazy val scalatestVersion = "3.0.7"

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
      "ch.megard" %% "akka-http-cors" % "0.3.0",
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % "test",
      "org.scalatest" %% "scalatest" % scalatestVersion % "test"
    )
  )
