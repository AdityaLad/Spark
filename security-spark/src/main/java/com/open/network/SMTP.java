package com.open.network;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class SMTP {
	
	public void process(SparkSession spark) {
		System.out.println("Reading smtp logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.option("header", "true").csv("resources/network-datasets/smtp.csv");
		df.printSchema();
		df.show();		
	}

}
