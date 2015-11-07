name := "chordial-daemon"

// Needs to be included in each build.sbt until Scalastyle is updated to correctly resolve settings
scalastyleConfig := baseDirectory.value / ".." / ".." / "project" / "scalastyle_config.xml"
