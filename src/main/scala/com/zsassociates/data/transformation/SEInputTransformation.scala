package com.zsassociates.data.transformation

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.zsassociates.data.functions._

case class SEInputTransformationOutput(hcpAttributes: Array[String])

object SEInputTransformation {

  var options: SEInputTransformationOptions = _

  val parser = new scopt.OptionParser[SEInputTransformationOptions]("data-transformation") {
    head("data-transformation", "0.1")

    opt[String]("hcp-detail")
      .required()
      .action((x, c) => c.copy(hcpDetailSource = x))
      .text("Path of HCP Master File")
      .valueName("<path>")

    opt[String]("hcp-segmentation")
      .action((x, c) => c.copy(hcpSegmentSource = x))
      .text("Path of HCP Segmentation File")
      .valueName("<path>")

    opt[String]("standard-metrics")
      .action((x, c) => c.copy(standardMetricSource = x))
      .text("Path of Standard Metrics File")
      .valueName("<path>")

    opt[String]("promotional-metrics")
      .action((x, c) => c.copy(promotionalMetricSource = x))
      .text("Path of Promotional Metrics File")
      .valueName("<path>")

    opt[String]("source-format")
      .required()
      .action((x, c) => c.copy(sourceFormat = x))
      .text("Source Files Format")
      .valueName("<format>")

    opt[String]("transform-location")
      .required()
      .action((x, c) => c.copy(transformLocation = x))
      .text("Path where transformed file will be plcaed")
      .valueName("<path>")

    opt[String]("transform-format")
      .required()
      .action((x, c) => c.copy(transformFormat = x))
      .text("Transformed File format")
      .valueName("<format>")

    opt[String]("json-loc")
      .action((x, c) => c.copy(jsonOutputLoc = x))
      .text("Path where generated JSON will be placed")
      .valueName("<path>")
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Transformation Script").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)

    parser.parse(args.toSeq, SEInputTransformationOptions()) match {
      case Some(config) =>
        options = config

        try {

          val result = new Transformation(sqlContext, options).transform()

          // Saving output on the location as specified
          writeOutput(result, options.transformLocation, options.transformFormat)

          // Collect HCP Attributes
//          val output = SEInputTransformationOutput(hcpAttributes)
//
//          implicit val formats = net.liftweb.json.DefaultFormats
//          val jsonString = write(output)
//          saveToFile(URI.create(_options.jsonOutputLoc), jsonString)
        }
        finally {
          sc.stop()
        }

      case None =>
        throw new Exception("Invalid options")
    }

  }

}

case class SEInputTransformationOptions(hcpDetailSource: String = "",
                                        hcpSegmentSource: String = "",
                                        standardMetricSource: String = "",
                                        promotionalMetricSource: String = "",
                                        sourceFormat: String = "",
                                        transformLocation: String = "",
                                        transformFormat: String = "",
                                        jsonOutputLoc: String = ""
                                       )
