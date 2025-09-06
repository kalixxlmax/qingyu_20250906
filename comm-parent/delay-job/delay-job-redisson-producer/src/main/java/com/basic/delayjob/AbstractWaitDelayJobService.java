package com.basic.delayjob;

import com.basic.delayjob.consumer.IPushWaitDealyJob;
import com.basic.delayjob.model.DelayJobStatus;
import com.basic.delayjob.model.WaitDelayJob;
import com.basic.delayjob.producer.IWaitDelayJobService;
import com.basic.delayjob.repository.DelayJobDO;
import com.basic.delayjob.repository.WaitDelayJobDO;
import com.basic.delayjob.repository.WaitDelayJobRepository;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@Slf4j
public abstract class AbstractWaitDelayJobService implements IWaitDelayJobService {






    @Autowired
    protected WaitDelayJobRepository waitDelayJobRepository;



    @Override
    public boolean publishWaitJob(WaitDelayJob job) {
        log.info("提交等待延时任务 ===> topic {} jobId {}  waitEndTime {} ",
                job.getTopic(),job.getJobId(),job.getWaitEndTime());

        if(StringUtil.isEmpty(job.getJobId())||0==job.getExecTime()){
            log.error("数据异常 {}", job.toString());
            return false;
        }

        try {
            job.setStatus(DelayJobStatus.WAIT_PUBLISH.getType());
            waitDelayJobRepository.addDelayJobToDB(job);
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            return false;
        }


    }

    @Override
    public boolean cancelWaitJob(String topic,String jobId) {

        log.info("取消等待延时任务 ===> topic {} jobId {}",
                topic,jobId);
        WaitDelayJobDO delayJob = waitDelayJobRepository.getDelayJobByJobId(jobId);
        if(null== delayJob){
            log.info("等待延时任务数据库不存在 topic {} jobId {} ",
                    topic,jobId);
            return false;
        }
        return 0<waitDelayJobRepository.deleteDelayJob(jobId);


    }

    public List<WaitDelayJobDO> getWaitJobList(long waitEndTime){
       return waitDelayJobRepository.queryListDelayJobByExecTimeExpired(waitEndTime);
    }
}
