package com.open.network;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.collect_set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.open.alerting.Alerts;
import com.open.config.Fmt;

/*
 * Source - https://www.secrepo.com/maccdc2012/notice.log.gz
 * Analysis - https://github.com/cyberdefendersprogram/MachineLearning/blob/master/Data_analysis/Network%20analysis/notice%20analysis.ipynb
 * Log format help - http://gauss.ececs.uc.edu/Courses/c6055/pdf/bro_log_vars.pdf
 * 
 * @Aditya Lad
 */

public class BRO {
	
	public void process(SparkSession spark) {
		System.out.println("Reading bro logs");
		Dataset<Row> df = spark.read().option("timestampFormat", Fmt.GENERIC)
				.option("header", "true").csv("resources/network-datasets/notice.csv");
		
		//convert epoch to date, if you need 
	    df = df.withColumn("ts", from_unixtime(col("ts"),"MM-dd-yyyy HH:mm:ss"));
		
		//multiple pattern matching for suspicious events
		df.filter(col("note").rlike("(reverse-shell|Password_Guessing)")).show(2000,false);
		
		//filter for individual events, concat the event names for each ip address
		Dataset<Row> df2 = df.filter(col("note")
				.rlike("Scan::Port_Scan|HTTP::SQL_Injection_Attacker|Scan::Address_Scan|SSH::Password_Guessing|Signatures::Sensitive_Signature"))
				.groupBy(col("src")).agg(collect_set("note"))
				.orderBy(col("src"));
		
		//count the total number of attack events for each ip address
		Dataset<Row> df3 = df.filter(col("note")
				.rlike("Scan::Port_Scan|HTTP::SQL_Injection_Attacker|Scan::Address_Scan|SSH::Password_Guessing|Signatures::Sensitive_Signature"))
				.groupBy(col("src")).count().alias("count")
				.orderBy(col("src"));
						
		//join the two datasets on ip address
		//thus we get a list of attacker ips, with the events they have caused and sorted by their count
		Dataset<Row> df4 = df2.join(df3, "src").orderBy(col("count").desc());
		df4.show(200, false);
		
		//push this to a live datastore, where it could cross referenced by other processors too
		//TODO push to redis or something similar where we maintain live stats
		
		//Push metrics/alerts
		Alerts.notify("BRO","Distinct attack IPs - " + df4.count());
		Alerts.notify("BRO","Port scanning attempts - " + df.filter(col("note").like("Scan::Port_Scan")).count());
		Alerts.notify("BRO","Password guessing attempts - " + df.filter(col("note").like("SSH::Password_Guessing")).count());
		Alerts.notify("BRO","SQL injection attempts - " + df.filter(col("note").like("HTTP::SQL_Injection_Attacker")).count());
		Alerts.notify("BRO","Reverse shell connections noticed - " + df.filter(col("note").like("Signatures::Sensitive_Signature")).count());		
		
	}

	

}
