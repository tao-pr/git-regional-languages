name         := "gitlang"
organization := "com.starcolon"
description  := "Analysis of programming language distribution over geospatial regions, just for fun"
homepage     := Some(url("https://github.com/starcolon/git-regional-languages"))

version := "0.0.1-SNAPSHOT"

scalaVersion   := "2.11.8"
scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0"
)

/**
 * Automated source formatting upon compile, for consistency and focused code
 * reviews.
 */
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(DoubleIndentClassDeclaration, true)
  .setPreference(PreserveDanglingCloseParenthesis, true)