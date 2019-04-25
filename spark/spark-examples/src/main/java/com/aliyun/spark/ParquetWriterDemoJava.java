package com.aliyun.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetOutputWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.unsafe.types.UTF8String;


public class ParquetWriterDemoJava {
    public static void main(String args[]) {
        String pathString = args[0];
        Path path = new Path(pathString);
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop-conf/hdfs-site.xml"));
        conf.addResource(new Path("/etc/hadoop-conf/core-site.xml"));
        StructType dataSchema = new StructType().add("a", StringType$.MODULE$).add("b", StringType$.MODULE$);
        ParquetOutputWriter parquetWriter = new ParquetOutputWriter(path, conf, dataSchema);

        Object[] record = new Object[dataSchema.size()];
        record[0] = UTF8String.fromString("test111");
        record[1] = UTF8String.fromString("test2222");
        parquetWriter.write(new GenericInternalRow(record));

        record[0] = UTF8String.fromString("test22222000");
        record[1] = UTF8String.fromString("t444888");
        parquetWriter.write(new GenericInternalRow(record));
        parquetWriter.close();
    }
}
