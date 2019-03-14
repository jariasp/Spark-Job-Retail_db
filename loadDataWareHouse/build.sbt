name := "loadDataWareHouse"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.44"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.3" % "provided"