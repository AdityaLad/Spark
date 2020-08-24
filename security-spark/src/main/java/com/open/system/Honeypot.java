package com.open.system;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class Honeypot {
	public void process(SparkSession spark) {
		System.out.println("Reading Honeypot logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.json("resources/system/honeypot.json.gz");
		df.printSchema();
		df.show();		
	}
}
