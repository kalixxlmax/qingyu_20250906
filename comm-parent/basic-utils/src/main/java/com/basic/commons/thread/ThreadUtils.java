package com.basic.commons.thread;


import com.basic.commons.task.TimerTask;
import com.basic.commons.thread.pool.DefaultThreadFactory;

import java.util.concurrent.*;


/**
 * @author chat 
 *
 */
public class ThreadUtils {
	
	public static final ScheduledExecutorService mThreadPool = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors()*2,
			DefaultThreadFactory.getInstance("basic-util"),new ThreadPoolExecutor.CallerRunsPolicy());;
	/**
	* @Description: TODO(立即执行 线程)
	* @param @param callback    参数
	 */
	public static void executeInThread(Callback callback){
		mThreadPool.execute(new Runnable() {
    		@Override
			public void run() {
    			callback.execute(Thread.currentThread().getName());
    		}
		});
	}
	public static void executeInThread(Callback callback,Object obj){
		mThreadPool.execute(new Runnable() {
    		@Override
			public void run() {
    			callback.execute(obj);
    		}
		});
	}
	
	/**
	* @Description: TODO(延时执行线程)
	* @param @param callback 延时 秒钟
	 */
	public static void executeInThread(Callback callback,long delay){
		mThreadPool.schedule(new Runnable() {
    		@Override
			public void run() {
    			callback.execute(Thread.currentThread().getName());
    		}
		}, delay, TimeUnit.SECONDS);
	}
	public static void executeCallback(Callback callback,long initialDelay,long period){
		
		mThreadPool.scheduleAtFixedRate(()->callback.execute(Thread.currentThread().getName()), initialDelay, period, TimeUnit.SECONDS);
	}
	public static void executeCallback(Callback callback,long initialDelay,long period,TimeUnit unit){
		
		mThreadPool.scheduleAtFixedRate(()->callback.execute(Thread.currentThread().getName()), initialDelay, period, unit);
	}
	public static void executeCallback(Runnable runnable,long initialDelay,long period,TimeUnit unit){
		
		mThreadPool.scheduleAtFixedRate(runnable, initialDelay, period, unit);
	}
    public static void executeRunnable(Runnable runnable,long initialDelay,long period){
		
		mThreadPool.scheduleAtFixedRate(runnable, initialDelay, period, TimeUnit.SECONDS);
	}
    public static void executeTimerTask(TimerTask runnable, long initialDelay, long period){
		
		mThreadPool.scheduleAtFixedRate(runnable, initialDelay, period, TimeUnit.SECONDS);
	}
    public static void executeCallback(TimerTask runnable,long initialDelay,long period,TimeUnit unit){
		
		mThreadPool.scheduleAtFixedRate(runnable, initialDelay, period, unit);
	}
	
	public static String stackTrace() {
		StackTraceElement[] elements = Thread.currentThread().getStackTrace();
		StringBuilder buf = new StringBuilder();
		for (StackTraceElement element : elements) {
			buf.append("\r\n	").append(element.getClassName()).append(".").append(element.getMethodName()).append("(").append(element.getFileName()).append(":")
					.append(element.getLineNumber()).append(")");
		}
		return buf.toString();
	}

	/**
	 * @Author chat
	 * @Description //TODO(用submit执行线程)
	 * @Date 18:43 2019/12/2
	 **/
	public static Future submitInThread(Callback callback){
		Future submit = mThreadPool.submit(() -> callback.execute(Thread.currentThread().getName()));
		return submit;
	}

}
