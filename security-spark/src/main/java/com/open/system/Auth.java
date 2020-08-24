package com.open.system;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class Auth {
	public void process(SparkSession spark) {
		System.out.println("Reading Auth/SSHD logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.text("resources/system/sshd-auth.log");
		df.printSchema();
		//Filter brute force auth attempts
		df.filter(col("value").rlike("(Too many authentication failures for|Invalid user)")).show(20,false);
		
		//Count total brute force auth attempts
		System.out.println(df.filter(col("value").rlike("(Too many authentication failures for|Invalid user)")).count());
	}

}
