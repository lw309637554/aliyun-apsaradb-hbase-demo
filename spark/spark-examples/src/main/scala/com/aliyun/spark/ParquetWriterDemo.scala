package com.aliyun.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetOutputWriter
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

object ParquetWriterDemo {
  def main(args: Array[String]): Unit = {
    val pathString = args(0)
    println("pathString:" + args(0))
    val path: Path = new Path(pathString)
    val conf = new Configuration
    conf.addResource(new Path("/etc/hadoop-conf/hdfs-site.xml"))
    conf.addResource(new Path("/etc/hadoop-conf/core-site.xml"))
    val dataSchema = new StructType().add("a", StringType).add("b", StringType)
    val parquetWriter = new ParquetOutputWriter(path, conf, dataSchema)
    parquetWriter.write(new GenericInternalRow(Array[Any](UTF8String.fromString("test111"), UTF8String.fromString("test2222"))))
    parquetWriter.write(new GenericInternalRow(Array[Any](UTF8String.fromString("test444"), UTF8String.fromString("test444444"))))
    parquetWriter.close()
  }
}
