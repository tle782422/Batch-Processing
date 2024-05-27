package org.hadoop.mapreduce.pagecount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PageCount implements WritableComparable<PageCount> {
	private String page;
	private int count;
		
	public void set(String page, int count) {
		this.page = page;
		this.count = count;
	}
	public String getPage() {
		return page;
	}
	public void setPage(String page) {
		this.page = page;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	@Override
	public int compareTo(PageCount o) {
	    // So sánh số lượng trang của hai đối tượng PageCount
	    int countDifference = o.getCount() - this.count;
	    
	    // Nếu số lượng trang giống nhau, so sánh theo tên trang
	    if (countDifference == 0) {
	        // So sánh tên trang của hai đối tượng PageCount
	        return this.page.compareTo(o.getPage());
	    } else {
	        // Nếu số lượng trang khác nhau, trả về sự khác biệt
	        return countDifference;
	    }
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.page);
		out.writeInt(this.count);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.page = in.readUTF();
		this.count = in.readInt();
	}
	
	@Override
	public String toString() {
		return this.page + "," + this.count;
	}
	
}
