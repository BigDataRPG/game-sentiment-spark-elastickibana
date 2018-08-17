name := "SparkMLProjects"

version := "0.1"

scalaVersion := "2.11.4"

val elastic4sVersion = "6.1.2"

val sparkVer = "2.2.1"

libraryDependencies ++= {

  Seq(
    "org.apache.spark" %% "spark-core" % "2.2.1",
    "org.apache.spark" %% "spark-sql" % "2.2.1",
    "org.elasticsearch" %% "elasticsearch-spark-20" % "6.1.3",
    "org.apache.spark" %% "spark-mllib" % "2.2.1"
  )
}


libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"

assemblyJarName in assembly := "pam-ds3.jar"

assemblyMergeStrategy in assembly := {
  case PathList("com",   "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com",   "squareup", xs @ _*) => MergeStrategy.last
  case PathList("com",   "sun", xs @ _*) => MergeStrategy.last
  case PathList("com",   "thoughtworks", xs @ _*) => MergeStrategy.last
  case PathList("commons-beanutils", xs @ _*) => MergeStrategy.last
  case PathList("commons-cli", xs @ _*) => MergeStrategy.last
  case PathList("commons-collections", xs @ _*) => MergeStrategy.last
  case PathList("commons-io", xs @ _*) => MergeStrategy.last
  case PathList("io",    "netty", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.last
  //  case PathList("org",   "apache", xs @ _*) => MergeStrategy.last
  //  case PathList("org",   "elasticsearch", xs @ _*) => MergeStrategy.last
  case PathList("org",   "codehaus", xs @ _*) => MergeStrategy.last
  case PathList("org",   "fusesource", xs @ _*) => MergeStrategy.last
  case PathList("org",   "mortbay", xs @ _*) => MergeStrategy.last
  case PathList("org",   "tukaani", xs @ _*) => MergeStrategy.last
  case PathList("xerces", xs @ _*) => MergeStrategy.last
  case PathList("xmlenc", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

