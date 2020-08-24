package com.open.network;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class C99Conn {
	public void process(SparkSession spark) {
		System.out.println("Reading Connection logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.option("delimiter", "\t")
				.text("resources/network-datasets/conn.log.gz");
		df.printSchema();
		df.show();	
		  
	}

}
