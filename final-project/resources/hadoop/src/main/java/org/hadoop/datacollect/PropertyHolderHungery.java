package org.hadoop.datacollect;

import java.util.Properties;

public class PropertyHolderHungery {
	private static Properties prop = new Properties();
	static {
		try {
		prop.load(PropertyHolderHungery.class
				.getClassLoader()
				.getResourceAsStream("collect.properties"));
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	public static Properties getProps() throws Exception {
		return prop;
	}
}
