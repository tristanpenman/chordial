name := "Chordial"

// Needs to be included in each build.sbt until Scalastyle is updated to correctly resolve settings
scalastyleConfig := baseDirectory.value / "project" / "scalastyle_config.xml"

lazy val commonSettings = Seq(
  organization := "com.tristanpenman",
  version := "0.0.1",
  scalaVersion := "2.11.7",
  scalacOptions := Seq("-feature", "-unchecked", "-deprecation")
)

lazy val akkaVersion = "2.3.14"
lazy val scalatestVersion = "2.2.4"
lazy val sprayVersion = "1.3.3"
lazy val sprayWebsocketVersion = "0.1.4"

lazy val core = project.in(file("modules/core"))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  ))

lazy val daemon = project.in(file("modules/daemon"))
  .dependsOn(core)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-remote" % akkaVersion,
    "com.wandoulabs.akka" %% "spray-websocket" % sprayWebsocketVersion,
    "io.spray" %% "spray-can" % sprayVersion,
    "io.spray" %% "spray-routing-shapeless2" % sprayVersion,
    "io.spray" %% "spray-testkit" % sprayVersion % "test",
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  ))

