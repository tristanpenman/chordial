name := "Chordial"

lazy val commonSettings = Seq(
  organization := "com.tristanpenman",
  version := "0.0.1",
  scalaVersion := "2.11.7",
  scalacOptions := Seq("-feature", "-unchecked", "-deprecation")
)

lazy val akkaVersion = "2.3.11"
lazy val scalatestVersion = "2.2.4"
lazy val sprayVersion = "1.3.3"

lazy val core = project.in(file("modules/core"))
  .settings(name := "chordial-core")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion
  ))

lazy val daemon = project.in(file("modules/daemon"))
  .dependsOn(core)
  .settings(name := "chordial-daemon")
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-remote" % akkaVersion,
    "io.spray" %% "spray-can" % sprayVersion,
    "io.spray" %% "spray-routing" % sprayVersion,
    "io.spray" %% "spray-testkit" % sprayVersion % "test",
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  ))
