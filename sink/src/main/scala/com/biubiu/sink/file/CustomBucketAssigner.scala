package com.biubiu.sink.file

import java.text.SimpleDateFormat
import java.util.Date

import com.biubiu.sink.parquet.model.ResultPOJO
import com.google.gson.{Gson, JsonElement}
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer

class CustomBucketAssigner extends BucketAssigner[String,String]{

  @transient
  lazy val  df = new SimpleDateFormat("yyyyMMdd/HH");
 // @transient
  //val gson = new Gson()

  override def getBucketId(element: String, context: BucketAssigner.Context): String = {
    val gson = new Gson()
    val obj = gson.fromJson(element, classOf[JsonElement]).getAsJsonObject
    df.format(new Date(obj.get("ts").getAsLong))
  }

  override def getSerializer: SimpleVersionedSerializer[String] = {
    SimpleVersionedStringSerializer.INSTANCE
  }
}
