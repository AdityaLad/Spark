package com.open.alerting;

public class Alerts {
	
	public static void notify(String component, String msg) {
		System.out.println("Alert:" + component + ":"+ msg);
	}

}
