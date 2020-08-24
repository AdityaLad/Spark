package com.open.network;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class SSH {
	
	public void process(SparkSession spark) {
		System.out.println("Reading ssh logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.option("header", "true").csv("resources/network-datasets/ssh.csv");
		df.printSchema();
		df.show();	
		//Usecase 1 - count by who is making the max requests
		
		//if the column name has ".", it throws an error, small hack is to use backticks or renaamed the columns
		//1
		df.groupBy("`id.orig_h`").count().alias("count").orderBy(col("count").desc()).show();
		
		//2
		df = df.withColumnRenamed("id.orig_h", "orig_h");
		df.groupBy("orig_h").count().alias("count").orderBy(col("count").desc()).show();
		
		//Usecase 2 - group by and count those who are making maximum nmap port scans
		df.filter(col("client").like("%Nmap%")).groupBy("orig_h").count().alias("count").orderBy(col("count").desc()).show();
	}

}
