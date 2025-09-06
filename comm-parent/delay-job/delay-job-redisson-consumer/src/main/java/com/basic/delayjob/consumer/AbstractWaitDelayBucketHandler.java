package com.basic.delayjob.consumer;

import com.basic.delayjob.handler.WaitDelayJobHanler;
import com.basic.delayjob.model.WaitDelayJob;
import com.basic.delayjob.producer.ISubmitJobService;
import com.basic.delayjob.repository.WaitDelayJobDO;
import com.basic.delayjob.repository.WaitDelayJobRepository;
import com.basic.mongodb.utils.BeanCopyUtils;
import com.basic.redisson.AbstractRedisson;
import com.basic.redisson.LockCallBack;
import com.basic.redisson.ex.LockFailException;
import com.basic.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author chat 
 * @version V1.0
 * @Description:
 * @date 2020/1/13 19:12
 */
@Slf4j
@Service
public class AbstractWaitDelayBucketHandler extends AbstractRedisson implements WaitDelayJobHanler {

    @Autowired
    private WaitDelayJobRepository waitDelayJobRepository;

    @Autowired
    private ISubmitJobService submitJobService;


    @Autowired
    private RedissonClient redissonClient;

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

    /*@Autowired
    private IPushWaitDealyJob iPushWaitDealyJob;*/
    /*
    @Autowired
    private IExecuteDealyJob iExecuteDealyJob;*/


    public final static String PULLWAITDELAYJOB_LOCK = "delay:pullwaitdelayjob_lock";

    @Override
    public void pullWaitDelayJob() {
        try {
            executeOnLock(PULLWAITDELAYJOB_LOCK, o -> {
                 pullWaitDelayJobOnLock();
                 return null;
            });
        }catch (LockFailException e){

        }catch (Exception e){
            log.error(e.getMessage());
        }

    }

    private void pullWaitDelayJobOnLock(){
        List<WaitDelayJobDO> list =waitDelayJobRepository.queryListDelayJobByWaitEndTime(DateUtil.currentTimeSeconds()+120);
        for(WaitDelayJobDO waitDelayJobDO : list){
            WaitDelayJob waitDelayJob = new WaitDelayJob();
            BeanCopyUtils.copyProperties(waitDelayJobDO,waitDelayJob);
            // 等待时间小于两个小时的
            if(waitDelayJob.getWaitEndTime() - DateUtil.currentTimeSeconds() <= 7200){
                submitJobService.submitJob(waitDelayJob,waitDelayJob.getExecTime());
                waitDelayJobRepository.deleteDelayJob(waitDelayJob.getJobId());
            }
        }
    }


}
