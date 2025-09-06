package com.basic.commons.thread.pool;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 *
 * @author chat 
 * 2017年4月4日 上午9:23:12
 */
public abstract class AbstractSynQueueRunnable<T> extends AbstractSynRunnable {
	

	/** The msg queue. */
	protected ConcurrentLinkedQueue<T> msgQueue = new ConcurrentLinkedQueue<>();

	/**
	 *
	 * @param executor
	 * @author tanyaowu
	 */
	public AbstractSynQueueRunnable(Executor executor) {
		super(executor);
	}

	/**
	 * @return
	 *
	 */
	public boolean addMsg(T t) {
		if (this.isCanceled()) {
			return false;
		}

		return msgQueue.offer(t);
	}

	/**
	 * 清空处理的队列消息
	 */
	public void clearMsgQueue() {
		msgQueue.clear();
	}

	@Override
	public boolean isNeededExecute() {
		return !msgQueue.isEmpty();
	}
	

	//	/**
	//	 *
	//	 */
	//	@Override
	//	public ConcurrentLinkedQueue<T> getMsgQueue()
	//	{
	//		return msgQueue;
	//	}

}
