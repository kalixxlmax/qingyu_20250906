package com.basic.delayjob;

import com.basic.delayjob.constant.DelayJobConstant;
import com.basic.delayjob.model.DelayJob;

import com.basic.delayjob.model.DelayJobStatus;
import com.basic.delayjob.model.DelayJobType;
import com.basic.delayjob.model.ScoredSortedItem;
import com.basic.delayjob.producer.ISubmitJobService;
import com.basic.delayjob.repository.DelayJobDO;
import com.basic.delayjob.repository.DelayJobRepository;
import com.basic.redisson.AbstractRedisson;
import com.basic.utils.DateUtil;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public abstract class AbstractSubmitDelayJobService extends AbstractRedisson implements ISubmitJobService {






    @Autowired
    protected DelayJobRepository delayJobRepository;

    @Autowired
    protected RedissonClient redissonClient;


    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }


    /**
     * 获取delayBucket key 分开多个，有利于提高效率
     * @param jobId
     * @return
     */
    private  String getDelayBucketKey(String jobId) {
        return DelayJobConstant.DELAY_QUEUE_KEY_PREFIX +Math.floorMod(jobId.hashCode(),DelayJobConstant.DELAY_BUCKET_NUM);
    }


    private  String getDelayBucketKey() {
        return DelayJobConstant.DELAY_QUEUE_KEY_PREFIX;
    }

    @Override
    public boolean submitJob(DelayJob delayJob){
        return submitJob(delayJob,delayJob.getExecTime());
    }

    //@Transactional(rollbackFor = Exception.class)
    @Override
    public boolean submitJob(DelayJob job, Long execTime) {



        log.info("提交延时任务 ===> topic {} jobId {}  execTime {} ",
                job.getTopic(),job.getJobId(),job.getExecTime());

        if(StringUtil.isEmpty(job.getJobId())||0==job.getExecTime()){
            log.error("数据异常 {}", job.toString());
            return false;
        }

        if(!delayJobRepository.addDelayJobToDB(job)){
            log.info("延时任务已经提交 重复消费 topic {} jobId {} ",
                    job.getTopic(),job.getJobId());
            return false;
        }


        ScoredSortedItem item = new ScoredSortedItem(job.getJobId(), job.getExecTime());

        RScoredSortedSet<ScoredSortedItem> scoredSorteSet = getScoredSorteSet(getDelayBucketKey());

        scoredSorteSet.add(item.getDelayTime(),item);

        delayJobRepository.updateJobStatus(job.getJobId(), DelayJobStatus.WAITEXEC.getType());

        return true;



    }

   //@Transactional(rollbackFor = Exception.class)
    @Override
    public boolean cancelDelayJob(String topic,String jobId) {

        log.info("取消延时任务 ===> topic {} jobId {}",
                topic,jobId);
        DelayJobDO delayJob = delayJobRepository.getDelayJobByJobId(jobId);
        if(null== delayJob){
            log.info("延时任务数据库不存在 topic {} jobId {} ",
                    topic,jobId);
            return false;
        }
        RScoredSortedSet<ScoredSortedItem> scoredSorteSet = getScoredSorteSet(getDelayBucketKey());
        scoredSorteSet.remove(new ScoredSortedItem(delayJob.getJobId(),delayJob.getExecTime()));
        delayJobRepository.updateJobStatus(delayJob.getJobId(), DelayJobStatus.CANCEL.getType());
        return true;



    }
}
