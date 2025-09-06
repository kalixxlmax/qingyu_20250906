package com.basic.commons.thread.pool;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author chat 
 * 2017年4月4日 上午9:23:12
 */
public abstract class AbstractQueueRunnable<T> extends BaseRunnable {
	

	/** The msg queue. */
	protected ConcurrentLinkedQueue<T> msgQueue = new ConcurrentLinkedQueue<>();
	
	protected long sleep=1000;
	/**
	 * @param sleep the sleep to set
	 */
	public void setSleep(long sleep) {
		this.sleep = sleep;
	}
	
	protected int batchSize=1;
	/**
	 * @param batchSize the batchSize to set
	 */
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}
	
	/**
	 * 
	 * @author tanyaowu
	 */
	public abstract void runTask();
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		while(true) {
			try {
				synchronized (msgQueue){
					if(msgQueue.isEmpty()) {
						msgQueue.wait();
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
	public boolean addMsg(T t) {
		synchronized (msgQueue){
			 msgQueue.offer(t);
			 msgQueue.notifyAll();
		}
		return true;

	}
	
	/**
	 * 清空处理的队列消息
	 */
	public void clearMsgQueue() {
		msgQueue.clear();
	}

	public boolean isNeededExecute() {
		return !msgQueue.isEmpty();
	}
	
}
