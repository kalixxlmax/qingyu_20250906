package com.basic.commons.thread.pool;

import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author chat 
 * 2017年4月4日 上午9:23:12
 */
public abstract class AbstractMapRunnable<T> extends BaseRunnable {
	

	/** The msg queue. */
	protected ConcurrentHashMap<String,T> maps = new ConcurrentHashMap<String,T>();
	
	protected long sleep=3000;
	/**
	 * @param sleep the sleep to set
	 */
	public void setSleep(long sleep) {
		this.sleep = sleep;
	}
	
	protected int batchSize=20;
	/**
	 * @param batchSize the batchSize to set
	 */
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	/**
	 * 
	 */
	public abstract void runTask();
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		while(true) {
			try {
				synchronized (maps){
					if(maps.isEmpty()) {
						maps.wait();
					}
				}
				loopCount.set(0);
				runTask();

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	/**
	 * @return
	 *
	 */
	public void addMsg(String key,T t) {
		synchronized (maps){
			maps.put(key, t);
			maps.notifyAll();
		}
	}
	
	/**
	 * 清空处理的队列消息
	 */
	public void clearMsgQueue() {
		maps.clear();
	}

	public boolean isNeededExecute() {
		return !maps.isEmpty();
	}
	
}
