import AssemblyKeys._

assemblySettings

name := "template-scala-parallel-vanilla"

organization := "io.prediction"

libraryDependencies ++= Seq(
  "io.prediction"    %% "core"          % pioVersion.value % "provided",
  "org.apache.spark" %% "spark-core"    % "1.2.0" % "provided",
  "org.apache.spark" %% "spark-mllib"   % "1.2.0" % "provided",
  "org.scala-saddle" %% "saddle-core" % "1.3.+")

resolvers ++= Seq(
  "Sonatype Snapshots" at 
    "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at
    "http://oss.sonatype.org/content/repositories/releases"
)

