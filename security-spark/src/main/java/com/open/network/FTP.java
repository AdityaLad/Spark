package com.open.network;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class FTP {
	public void process(SparkSession spark) {
		System.out.println("Reading ftp logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.option("header", "true").csv("resources/network-datasets/ftp.csv");
		df.printSchema();
		df.show();		
	}

}
