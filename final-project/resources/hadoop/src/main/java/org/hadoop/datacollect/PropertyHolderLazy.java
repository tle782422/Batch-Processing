package org.hadoop.datacollect;

import java.util.Properties;

public class PropertyHolderLazy {
	// Design Pattern Singleton Khởi tạo khi chương trình chạy
	private static Properties prop = null;
	// Khởi tạo thuộc tính được lấy từ thư mục collect
	public static Properties getProps() throws Exception {	
		if(prop == null) {
		prop = new Properties();
		synchronized(PropertyHolderLazy.class) {
			if(prop==null) {
				prop.load(PropertyHolderLazy.class
						.getClassLoader()
						.getResourceAsStream("collect.properties"));
				}
			}
		}	
		return prop;
	}
}
