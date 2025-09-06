package com.basic.im.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author lidaye
 *
 */
@Setter
@Getter
public class PressureParam {

	
	
	private  int checkNum=1;// 每个群组发送者总人数：
	
	private int sendMsgNum=1;
	
	private String roomJid="";
	
	private int timeInterval=10;

	private List<String> jids;
	
	private AtomicInteger atomic;

	private CountDownLatch coutDuwn;
	
	private int sendAllCount;
	
	private List<Object> conns;
	
	private long startTime;
	
	
	private String timeStr;
	

	
	@Setter
	@Getter
	public static class PressureResult{
		private int sendAllCount;// 发送总条数
		
		private long timeCount;// 总用时
		
		private String TimeStr;// 当前批次
	}
	
}
