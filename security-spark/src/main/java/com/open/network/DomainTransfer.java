package com.open.network;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

public class DomainTransfer {
	public void process(SparkSession spark) {
		System.out.println("Reading Domaintransfer logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.json("resources/network-datasets/domainmovement/");
		df = df.select("resolve.change","resolve.changeNS","resolve.date","resolve.domainName","resolve.ip");    
		df.printSchema();
		df.show();	
		  
	}


}
