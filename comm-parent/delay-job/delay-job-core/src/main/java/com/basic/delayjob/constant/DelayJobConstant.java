package com.basic.delayjob.constant;

public class DelayJobConstant {

  /**
   * 过期时间
   */
  public final static String DELAY_QUEUE_KEY_PREFIX = "delay:job:expire";

  /**
   * BUCKET 个数
   */
  public final static int DELAY_BUCKET_NUM = 10;

  /**
   * 发布延时任务主题
   */
  public final static String PUBLISH_DELAYJOB_TOPIC="publish-delayjob";

  /**
   * 发布延时任务消费者组
   */
  public final static String PUBLISH_DELAYJOB_CONSUMER_GROUP="publish-delayjob-consumer";


  /**
   * 发布待延时任务主题
   */
  public final static String PUBLISH_WAITDELAYJOB_TOPIC="publish-waitdelayjob";


  /**
   * 发布待延时任务消费者组
   */
  public final static String PUBLISH_WAIT_DELAY_JOB_CONSUMER_GROUP="publish-waitdelayjob-consumer";


}
