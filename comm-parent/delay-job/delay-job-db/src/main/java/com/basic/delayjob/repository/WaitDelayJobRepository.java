package com.basic.delayjob.repository;

import com.basic.delayjob.model.WaitDelayJob;
import com.basic.mongodb.springdata.IBaseMongoRepository;

import java.util.List;

public interface WaitDelayJobRepository extends IBaseMongoRepository<WaitDelayJobDO, String> {



    WaitDelayJobDO getDelayJobByJobId(String jobId);

    boolean addDelayJobToDB(WaitDelayJob delayJob);

    long deleteDelayJob(String jobId);


    List<WaitDelayJobDO> queryListDelayJobByTopic(String topic);

    List<WaitDelayJobDO> queryListDelayJobByExecTimeExpired(long time);

    List<WaitDelayJobDO> queryListDelayJobByWaitEndTime(long time);

    List<WaitDelayJobDO> queryListDelayJobByTtlTimeExpired(long time);

    long deleteDelayJobAndTopic(String jobId, String topic);

    long deleteDelayJobByTopic(String topic);

    long deleteDelayJobByWaitEndTimeExpired(long time);

    long deleteDelayJobByTtlTimeExpired(long time);

    long updateJobStatus(String jobId, byte status);
}
