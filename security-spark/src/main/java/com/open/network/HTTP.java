package com.open.network;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class HTTP {
	public void process(SparkSession spark) {
		System.out.println("Reading HTTP logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.option("header", "false")
				.option("delimiter", "\t")
				.csv("resources/network-datasets/http.log");
		df.printSchema();
		//convert epoch to date 
		df.withColumn("_c0", from_unixtime(col("_c0"),"MM-dd-yyyy HH:mm:ss")).show();	
	}

}
