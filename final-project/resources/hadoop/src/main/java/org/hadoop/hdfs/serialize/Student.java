package org.hadoop.hdfs.serialize;

import java.io.Serializable;

public class Student implements Serializable {
	private static final long serialVersionUID = 366375112128381238L;
	private String SID;
	private String name;
	private String age;
	public Student() {
		// TODO Auto-generated constructor stub
	}
	public Student(String sID, String name, String age) {
		super();
		SID = sID;
		this.name = name;
		this.age = age;
	}
	public String getSID() {
		return SID;
	}
	public void setSID(String sID) {
		SID = sID;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAge() {
		return age;
	}
	public void setAge(String age) {
		this.age = age;
	}
	
}
