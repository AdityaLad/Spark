package com.open.system;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class Squid {
	public void process(SparkSession spark) {
		System.out.println("Reading Squid logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.option("delimiter", " ")
				.csv("resources/system/squid-access.log.gz");
		
		//convert epoch to date 
		df.withColumn("_c0", from_unixtime(col("_c0"),"MM-dd-yyyy HH:mm:ss")).show();			
	}

}
