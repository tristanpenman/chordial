lazy val scalastyleVersion = "0.7.0"

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % scalastyleVersion)

resolvers += "Spray" at "http://repo.spray.io"
resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"
