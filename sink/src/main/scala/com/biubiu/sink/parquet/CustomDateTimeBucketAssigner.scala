package com.biubiu.sink.parquet

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.Date

import com.biubiu.sink.parquet.model.ResultPOJO
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner

/*
* Class to override partition directory
*/
case class CustomDateTimeBucketAssigner(formatString: String) extends DateTimeBucketAssigner[ResultPOJO](formatString) {

  @transient
  lazy val  df = new SimpleDateFormat("yyyyMMdd/HH");

  override def getBucketId(element: ResultPOJO, context: BucketAssigner.Context): String = {
    df.format(new Date(element.ts))
  }
}
