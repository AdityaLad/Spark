package com.open.network;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class BRO {
	
	public void process(SparkSession spark) {
		System.out.println("Reading bro logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.option("header", "true").csv("resources/network-datasets/notice.csv");
		df.printSchema();
		
		//multiple pattern matching
		df.filter(col("note").rlike("(reverse-shell|Password_Guessing)")).show(2000,false);
		
		//or individual
		Dataset<Row> df2 = df.filter(col("note").like("%Password_Guessing%"));
		Dataset<Row> df3 = df.filter(col("msg").like("%reverse-shell%"));
		df = df2.union(df3);
		
		df.show();		
		//convert epoch to date 
		df.withColumn("ts", from_unixtime(col("ts"),"MM-dd-yyyy HH:mm:ss")).show();
	}

	

}
