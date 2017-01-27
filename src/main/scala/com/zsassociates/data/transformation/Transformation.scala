package com.zsassociates.data.transformation

import com.zsassociates.data.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import com.zsassociates.data.transformation.ColumnNames._

/**
  * Created by M.Kumar on 1/23/2017.
  */
class Transformation (sqlContext : SQLContext, _options : SEInputTransformationOptions)  {

  def transform(): DataFrame = {


    val hcpDetails = loadFileAsDataFrame(sqlContext, _options.hcpDetailSource, _options.sourceFormat)
    val hcpSegmentDataFrame = transformHcpSegments()
    val metricsDataFrame = transformMetrics()

    val metricAndSegment = if (hcpSegmentDataFrame != null && metricsDataFrame != null)
      hcpSegmentDataFrame.join(metricsDataFrame, Seq(HcpId, ProductId))
    else if (hcpSegmentDataFrame != null) hcpSegmentDataFrame
    else if (metricsDataFrame != null) metricsDataFrame
    else null

    val result = if (metricAndSegment != null)
      hcpDetails.join(metricAndSegment, Seq(HcpId))
    else
      hcpDetails

    result
  }

  def transformHcpSegments(): DataFrame = {

    if (!isNullOrEmpty(_options.hcpSegmentSource)) {
      loadFileAsDataFrame(sqlContext, _options.hcpSegmentSource, _options.sourceFormat)
        .withColumn("SEG_SOURCE_NAME", concat(lower(col(SegmentSource)), lit("_"), lower(col(SegmentName))))
        .groupBy(HcpId, ProductId).pivot("SEG_SOURCE_NAME").agg(first(SegmentValue))
    }
    else
      null
  }

  def transformMetrics(): DataFrame = {

    val metricsDf = if (!isNullOrEmpty(_options.standardMetricSource) && !isNullOrEmpty(_options.promotionalMetricSource))
      transformStandardMetrics()
        .unionAll(transformPromotionalMetrics())
    else if (!isNullOrEmpty(_options.standardMetricSource))
      transformStandardMetrics()
    else if (isNullOrEmpty(_options.promotionalMetricSource))
      transformPromotionalMetrics()
    else
      null

    if (metricsDf != null) {
      metricsDf
        .withColumn("METRIC_SK_NEW", concat(lit("metric"), col(MetricId)))
        .withColumn("METRIC_VALUE_NEW", col(MetricValue).cast(DoubleType))
        .groupBy(ProductId, HcpId, RepId, SalesforceId, TerritoryId, TimeBucketId)
        .pivot("METRIC_SK_NEW")
        .agg(sum("METRIC_VALUE_NEW"))
        .na.fill(0.0d) //Filling missing Metric value as 0
    }
    else
      null

  }

  def transformPromotionalMetrics(): DataFrame = {

    val source = _options.promotionalMetricSource
    if (!isNullOrEmpty(source)) {
      val promotionalMetrics = loadFileAsDataFrame(sqlContext, source, _options.sourceFormat)
      val modifiedPromotionalMetrics = promotionalMetrics.withColumn(TimeBucketId, lit(0)).withColumn(TerritoryId, lit(0))
      val modifiedPromoMetricsCols = modifiedPromotionalMetrics.columns
      scala.util.Sorting.quickSort(modifiedPromoMetricsCols)
      modifiedPromotionalMetrics.selectExpr(modifiedPromoMetricsCols: _*)
    }
    else
      null
  }

  def transformStandardMetrics(): DataFrame = {

    val source = _options.standardMetricSource
    if (!isNullOrEmpty(source)) {
      val stdView = loadFileAsDataFrame(sqlContext, source, _options.sourceFormat)
      //Adding REP_SK and TERRITORY_SK with Standard View/Table and Sorting the columns
      val modifiedStdView = stdView.withColumn(RepId, lit(-2)).withColumn(TerritoryId, lit(-2)).withColumn(SalesforceId, lit(-2))
      val modifiedStdViewCols = modifiedStdView.columns
      scala.util.Sorting.quickSort(modifiedStdViewCols)
      modifiedStdView.selectExpr(modifiedStdViewCols: _*)
    }
    else
      null
  }

  private def hcpAttributes: Array[String] = {

    val hcpDetailColumns = loadFileAsDataFrame(sqlContext, _options.hcpDetailSource, _options.sourceFormat).schema
      .fieldNames.filter(_ != HcpId)
    val hcpSegmentDf = transformHcpSegments()

    if (hcpSegmentDf != null)
      hcpSegmentDf.schema.fieldNames.filter(f => f != HcpId && f != ProductId) ++ hcpDetailColumns
    else
      hcpDetailColumns
  }

}

