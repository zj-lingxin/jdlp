name := "jdlp"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.poi" % "poi" % "3.14"

libraryDependencies += "org.apache.poi" % "poi-ooxml" % "3.14"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"

libraryDependencies += "com.rabbitmq" % "amqp-client" % "3.5.4"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.36"

libraryDependencies += "com.qcloud" % "cos_api" % "3.1"