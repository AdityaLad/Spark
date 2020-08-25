package com.open.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.open.malware.Apt1;
import com.open.malware.Zeus;
import com.open.network.BRO;
import com.open.network.Connections;
import com.open.network.DomainTransfer;
import com.open.network.FTP;
import com.open.network.HTTP;
import com.open.network.SMTP;
import com.open.network.SSH;
import com.open.system.Auth;
import com.open.system.Honeypot;
import com.open.system.Squid;

/**
 * Hello Security Spark world!
 * 
 * This is simple code write up for processing security logs in Spark. Should be good enough for a student, learner 
 * or a security practitioner.
 * 
 * Picking up the log samples hosted at - https://www.secrepo.com/ , reading them via Spark and running some 
 * basic checks to see if we find anything suspicious. 
 * 
 * If you find juicy stuff, you can write it to an S3 bucket (security datalake), ELK stack (faster analysis) or some other temp datastore for correlation with
 * other suspicious alerts. And you can create your own small SIEM.
 * 
 * You can even bundle this jar and its dependencies to a docker images and run it over a K8 cluster and schedule it
 * through Airflow or some other batch orchestration tool to create an ETL. 
 * 
 * I intend to create this to have some working code for developing POCs, collection of useful spark functions for security data 
 * processing use-cases. 
 * 
 * @Aditya Lad
 * 
 *
 */
public class App 
{
    public static void main( String[] args )
    {   
   
    SparkConf conf = new SparkConf().setAppName("SparkProject-SecurityLogs").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
     
    SparkSession spark = SparkSession.builder().appName("SparkSession")
    .config(conf).getOrCreate();
    
    SSH ssh = new SSH();
    //ssh.process(spark);
    
    FTP ftp = new FTP();
    //ftp.process(spark);
    
    SMTP smtp = new SMTP();
    //smtp.process(spark);
    
    BRO bro = new BRO();
    //bro.process(spark);
    
    HTTP http = new HTTP();
    //http.process(spark);
    
    Squid squid = new Squid();
    //squid.process(spark);
    
    Auth auth = new Auth();
    //auth.process(spark);
    
    DomainTransfer domainTransfer = new DomainTransfer();
    //domainTransfer.process(spark);
    
    Connections conn = new Connections();
    //conn.process(spark);
    
    Honeypot honeypot = new Honeypot();
    //honeypot.process(spark);
    
    Zeus zeus = new Zeus();
    zeus.process(spark);
    
    Apt1 apt1 = new Apt1();
    apt1.process(spark);
    
    sc.stop();
    }
   
}
