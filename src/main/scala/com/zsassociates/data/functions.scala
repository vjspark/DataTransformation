package com.zsassociates.data

import java.net.URI
import java.sql.{Connection, DriverManager}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


/**
  * Created by mk13935 on 5/9/2016.
  */
object functions {

  def truncateTable(tableName: String, conn: Connection): Int = {
    val sql = s"DELETE FROM $tableName"
    val preparedStatement = conn.prepareStatement(sql)
    preparedStatement.executeUpdate()
  }

  def getConnectionString(hostName: String, serviceName: String, port: Int): String = s"jdbc:oracle:thin:@$hostName:$port:$serviceName"

  def getConnection(connString: String, username: String, password: String): Connection = DriverManager.getConnection(connString, username, password)

  def loadFileAsDataFrame(sqlContext: SQLContext, filePath: String, fileFormat: String): DataFrame = {
    if (fileFormat.equalsIgnoreCase("csv")) {
      sqlContext.read.format("com.databricks.spark.csv").
        option("header", "true").
        option("inferSchema", "true").
        option("delimiter", ",").
        load(filePath)
    }
    else if (fileFormat.equalsIgnoreCase("parquet") || fileFormat.equalsIgnoreCase("json")) {
      sqlContext.read.format(fileFormat).load(filePath)
    }
    else {
      throw new Exception("%s input format is not supported".format(fileFormat))
    }
  }

  def getSubDirectories(sourcePath: String): Array[FileStatus] = {
    val uriPath = URI.create(sourcePath)
    val dir = FileSystem.get(uriPath, new Configuration())
    dir.listStatus(new Path(uriPath)).filter(f => f.isDirectory)
  }


  def writeOutput(output: DataFrame, outFilePath: String, outFileFormat: String): Unit = {
    if (outFileFormat.equalsIgnoreCase("csv")) {
      output
        .write
        .mode(SaveMode.Overwrite)
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .save(outFilePath)
    }
    else if (outFileFormat.equalsIgnoreCase("parquet") || outFileFormat.equalsIgnoreCase("json")) {
      output
        .write
        .mode(SaveMode.Overwrite)
        .format(outFileFormat)
        .save(outFilePath)
    }
    else {
      throw new Exception("%s output format is not supported".format(outFileFormat))
    }
  }

  def isNullOrEmpty(value: String): Boolean = value == null || value == ""

  def saveToFile(saveLoc: URI, data: String) = {
    val path = new Path(saveLoc)
    val conf = new Configuration()
    val fs = FileSystem.get(saveLoc, conf)
    val outputStream = fs.create(path, true)
    try {
      IOUtils.write(data, outputStream)
      outputStream.flush()
    }
    finally {
      outputStream.close()
    }
  }


}
