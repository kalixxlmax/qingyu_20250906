package com.basic.delayjob;

import com.basic.delayjob.model.DelayJob;
import com.basic.delayjob.model.DelayJobType;
import com.basic.delayjob.model.WaitDelayJob;
import com.basic.utils.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Service
public class SubmitDelayJobService extends AbstractSubmitDelayJobService {

    @Lazy
    @Autowired
    private AbstractWaitDelayJobService waitDelayJobService;



    @Override
    public boolean submitJob(DelayJob delayJob, Long delay) {
        if(delayJob.getExecTime() - DateUtil.currentTimeSeconds() < 7200) {
           return super.submitJob(delayJob,delayJob.getExecTime());
        }else {
           return waitDelayJobService.publishWaitJob(new WaitDelayJob(delayJob));
        }
    }

    @Override
    public boolean cancelDelayJob(String topic, String jobId) {
        boolean result = super.cancelDelayJob(topic, jobId);
        if(!result){
            waitDelayJobService.cancelWaitJob(topic,jobId);
        }
        return result;
    }
}
