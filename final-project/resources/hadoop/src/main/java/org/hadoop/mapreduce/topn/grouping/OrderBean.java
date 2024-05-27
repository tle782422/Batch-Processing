package org.hadoop.mapreduce.topn.grouping;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {

	private String orderId;
	private String userId;
	private String productName;
	private float price;
	private int quantity;
	private float amountFee;
	
	
	public void set(String orderId, String userId, String productName, float price, int quantity) {
		this.orderId = orderId;
		this.userId = userId;
		this.productName = productName;
		this.price = price;
		this.quantity = quantity;
		this.amountFee = price*quantity;
	}
	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getProductName() {
		return productName;
	}
	public void setProductName(String productName) {
		this.productName = productName;
	}
	public float getPrice() {
		return price;
	}
	public void setPrice(float price) {
		this.price = price;
	}
	public int getQuantity() {
		return quantity;
	}
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
	public float getAmountFee() {
		return amountFee;
	}
	public void setAmountFee(int amountFee) {
		this.amountFee = amountFee;
	}
	
	@Override
	public String toString() {
		return this.orderId + "," + this.userId + "," + this.productName + "," + this.price + "," + this.quantity + "," + this.amountFee;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.orderId);
		out.writeUTF(this.userId);
		out.writeUTF(this.productName);
		out.writeFloat(this.price);
		out.writeInt(this.quantity);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readUTF();
		this.userId = in.readUTF();
		this.productName = in.readUTF();
		this.price = in.readFloat();
		this.quantity = in.readInt();
		this.amountFee = (this.price *this.quantity);	
	}
	
	@Override
	public int compareTo(OrderBean o) {
	   return this.orderId.compareTo(o.getOrderId())==0?Float.compare(o.getAmountFee(), this.getAmountFee()):this.orderId.compareTo(o.getOrderId());
	}
}
