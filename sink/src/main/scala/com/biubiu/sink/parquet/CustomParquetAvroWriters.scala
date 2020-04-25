package com.biubiu.sink.parquet

import java.io.IOException

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.reflect.ReflectData
import org.apache.flink.formats.parquet.{ParquetBuilder, ParquetWriterFactory}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.OutputFile

/*
* Refer to @ParquetAvroWriters
* The above class does not have many options of ParquetWriter.
* https://github.com/apache/flink/pull/7547
*/
object CustomParquetAvroWriters {
//  def forReflectRecord[T](): ParquetWriterFactory[T] = {
//    val schemaString = ReflectData.get.getSchema(classOf[T]).toString
//    val builder = new ParquetBuilder[T] {
//      override def createWriter(out: OutputFile): ParquetWriter[T] = createAvroParquetWriter(schemaString, ReflectData.get, out)
//    }
//    new ParquetWriterFactory[T](builder)
//  }



  @throws[IOException]
  private def createAvroParquetWriter[T](schemaString: String, dataModel: GenericData, out: OutputFile):ParquetWriter[T] = {
    val schema = new Schema.Parser().parse(schemaString)
    AvroParquetWriter.builder[T](out).withSchema(schema).withDataModel(dataModel).withCompressionCodec(CompressionCodecName.SNAPPY).build()
  }
}
