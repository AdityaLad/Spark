package com.open.network;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.config.Fmt;

/*
 * Log src - http://www.secrepo.com/Security-Data-Analysis/Lab_1/conn.log.zip
 * Analysis & field description - https://github.com/sooshie/Security-Data-Analysis/blob/master/Lab_1/Lab_1-Solutions.ipynb
 */

public class Connections {
	public void process(SparkSession spark) {
		System.out.println("Reading Connection logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.option("delimiter", "\t")
				.option("header", false)
				.csv("resources/network-datasets/conn.log.gz");
		df.printSchema();
		
		/*Set a few header names, as there is no header in the log file*/
		df = df.withColumnRenamed("_c0", "ts")
				.withColumnRenamed("_c1", "uid")
				.withColumnRenamed("_c2", "id.orig_h")
				.withColumnRenamed("_c3", "id.orig_p")
				.withColumnRenamed("_c4", "id.resp_h")
				.withColumnRenamed("_c5", "id.resp_p")
				.withColumnRenamed("_c6", "proto")
				.withColumnRenamed("_c7", "service")
				.withColumnRenamed("_c8", "duration")
				.withColumnRenamed("_c9", "orig_bytes")
				.withColumnRenamed("_c10", "resp_bytes")
				.withColumnRenamed("_c11", "conn_state")
				.withColumnRenamed("_c12", "local_orig")
				.withColumnRenamed("_c13", "missed_bytes")
				.withColumnRenamed("_c14", "history")
				.withColumnRenamed("_c15", "orig_pkts")
				.withColumnRenamed("_c16", "orig_ip_bytes")
				.withColumnRenamed("_c17", "resp_pkts")
				.withColumnRenamed("_c18", "resp_ip_bytes")
				.withColumnRenamed("_c19", "tunnel_parents");
		
		
		/*Convert epoch to a human readable timestamp*/
		df = df.withColumn("ts", from_unixtime(col("ts"),"MM-dd-yyyy HH:mm:ss"));
		df.show();	
		
		/* get a list of top origin ips who are sending max traffic sorted by protocol*/
		df.groupBy(col("`id.orig_h`").as("orig_src"),col("proto").as("nw_proto"),col("service").as("app_service")).count().as("count").orderBy(col("count").desc()).show();
		
		/* get ssl connections not happening over server port 443*/
		df.filter(col("service").like("ssl")).filter(col("`id.resp_p`").notEqual("443")).show();
		 
	}

}
