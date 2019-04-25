package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.types.StructType

class ParquetOutputWriter(path: Path, conf: Configuration, dataSchema: StructType) extends Logging  {
  private var recordWriter: RecordWriter[Void, InternalRow] = null

  private def prepareRecordWrite(): Unit ={
    val WRITE_SUPPORT_CLASS = "parquet.write.support.class"
    conf.set(WRITE_SUPPORT_CLASS, classOf[org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport].getName)

    ParquetWriteSupport.setSchema(dataSchema, conf)
    conf.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,false)
    conf.set(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      ParquetOutputTimestampType.INT96.toString)
    // Sets compression scheme
    conf.set(ParquetOutputFormat.COMPRESSION, "snappy")

    // SPARK-15719: Disables writing Parquet summary files by default.
    if (conf.get(ParquetOutputFormat.ENABLE_JOB_SUMMARY) == null) {
      conf.setBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, false)
    }
    val codec: CompressionCodecName = CompressionCodecName.fromConf(conf.get(ParquetOutputFormat.COMPRESSION, UNCOMPRESSED.name))
    logInfo(s"""path= ${path.toUri.toString},dataSchema=${dataSchema.json},compressioncode=${codec.getHadoopCompressionCodecClassName} """)
    this.recordWriter=new ParquetOutputFormat[InternalRow]().getRecordWriter(conf,path,codec)
  }
  prepareRecordWrite()

  def write(row: InternalRow): Unit = recordWriter.write(null, row)

  def close(): Unit = recordWriter.close(null)


}
