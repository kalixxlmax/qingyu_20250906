package com.basic.delayjob.consumer;

import com.alibaba.fastjson.JSON;
import com.basic.delayjob.constant.DelayJobConstant;
import com.basic.delayjob.handler.DelayJobHanler;
import com.basic.delayjob.model.DelayJob;
import com.basic.delayjob.model.DelayJobStatus;
import com.basic.delayjob.model.ScoredSortedItem;
import com.basic.delayjob.repository.DelayJobDO;
import com.basic.delayjob.repository.DelayJobRepository;
import com.basic.mongodb.utils.BeanCopyUtils;
import com.basic.redisson.AbstractRedisson;
import com.basic.redisson.ex.LockFailException;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.redisson.RedissonShutdownException;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractDelayBucketHandler extends AbstractRedisson implements DelayJobHanler, Runnable {


    protected volatile RScoredSortedSet<ScoredSortedItem> scoredSorteSet;

    @Autowired
    protected DelayJobRepository delayJobRepository;

    @Autowired
    protected IExecuteDealyJob executeDealyJob;

    private String getDelayBucketKey() {
        return DelayJobConstant.DELAY_QUEUE_KEY_PREFIX;
    }


    public final static String PULLDELAYJOB_LOCK = "delay:pullDelayJob_lock";

    private Random random=new Random();

    @Override
    public void run() {
        try {
            log.info("定期拉取延时任务开启,刚启动先休息10秒");
            TimeUnit.SECONDS.sleep(20L);
            scoredSorteSet = getRedissonClient().getScoredSortedSet(getDelayBucketKey());
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }

        while (true) {
            try {
                if (getRedissonClient().isShutdown()){
                    log.info("RedissonClient isShutdown");
                    sleep(10);
                }
                TimeUnit.MILLISECONDS.sleep(random.nextInt(3000));
                pullDelayJob();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

    }


    private void sleep(long sleep) {
        try {
            TimeUnit.SECONDS.sleep(sleep);
        } catch (InterruptedException e) {
            log.error("", e);
        }
    }

    @Override
    public void pullDelayJob() {
        try {

            executeOnLock(PULLDELAYJOB_LOCK, o -> {
                pullDelayJobOnLock();
                return null;
            });
        }catch (LockFailException e){
            sleep(random.nextInt(20)); // 连接失败，随机休眠20秒
        }catch (RedissonShutdownException e) {
            log.error(e.getMessage(),e);
            sleep(random.nextInt(30)); // 连接失败，随机休眠30秒
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }

    }


    private void pullDelayJobOnLock(){
        try {
            ScoredSortedItem item = scoredSorteSet.first();
            log.debug("pullDelayJob ");
            // 没有任务
            if (null == item) {
                sleep(5);
                log.debug("pullDelayJob sleep 5");
                return;
            }
            // 延时时间没有到
            if (item.getDelayTime() > DateUtil.currentTimeSeconds()) {
                log.debug("pullDelayJob sleep 2");
                sleep(2);
                return;
            }
            if (StringUtil.isEmpty(item.getJobId()) || 0 == item.getDelayTime()) {
                log.error("数据异常 {}", JSON.toJSONString(item));
                return;
            }
            DelayJobDO delayJob = delayJobRepository.getDelayJobByJobId(item.getJobId());
            if (null == delayJob) {
                log.info("延时任务数据库不存在 移除任务 ==> jobId {} delayTime {} ",
                        item.getJobId(), item.getDelayTime());
                scoredSorteSet.remove(item);
                return;
            } else if (delayJob.getStatus() == DelayJobStatus.CANCEL.getType() ||
                    delayJob.getStatus() > DelayJobStatus.WAITEXEC.getType()) {
                log.info("任务状态异常,取消执行 status {} jobId {}",
                        DelayJobStatus.getAddTypeDesc(delayJob.getStatus()), item.getJobId());

                //删除旧的
                scoredSorteSet.remove(item);
                //重新计算延迟时间
                //scoredSorteSet.add(delayJob.getExecTime(),new ScoredSortedItem(delayJob.getJobId(),delayJob.getExecTime()));

            } else if (delayJob.getExecTime() > DateUtil.currentTimeSeconds()) {
                log.info("任务执行时间不对,重新计算 db time {} redis time {}  jobId {}",
                        delayJob.getExecTime(), item.getDelayTime(), item.getJobId());

                //删除旧的
                scoredSorteSet.remove(item);
                //重新计算延迟时间
                scoredSorteSet.add(delayJob.getExecTime(), new ScoredSortedItem(delayJob.getJobId(), delayJob.getExecTime()));

            } else {
                log.info("任务执行时间已到 执行任务 topic {} jobId {} execTime {}",
                        delayJob.getTopic(), delayJob.getJobId(), delayJob.getExecTime());

                DelayJob execjob = new DelayJob();
                BeanCopyUtils.copyProperties(delayJob, execjob);
                executeDealyJob.execute(execjob);

                scoredSorteSet.remove(item);
                delayJobRepository.deleteDelayJobAndTopic(delayJob.getJobId(), delayJob.getTopic());
            }
        } catch (RedissonShutdownException e) {
            sleep(20); // 连接失败，休眠20秒
        }
    }
}
