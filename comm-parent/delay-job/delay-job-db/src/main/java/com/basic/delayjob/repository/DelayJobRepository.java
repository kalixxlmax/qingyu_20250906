package com.basic.delayjob.repository;

import com.basic.delayjob.model.DelayJob;
import com.basic.mongodb.springdata.IBaseMongoRepository;

import java.util.List;

public interface DelayJobRepository extends IBaseMongoRepository<DelayJobDO, String> {



    DelayJobDO getDelayJobByJobId(String jobId);

    boolean addDelayJobToDB(DelayJob delayJob);

    long deleteDelayJob(String jobId);

    long deleteDelayJobAndTopic(String jobId, String topic);

    List<DelayJobDO> queryListDelayJobByTopic(String topic);

    List<DelayJobDO> queryListDelayJobByExecTimeExpired(long time);

    List<DelayJobDO> queryListDelayJobByTtlTimeExpired(long time);

    long deleteDelayJobByTopic(String topic);

    long deleteDelayJobByExecTimeExpired(long time);

    long deleteDelayJobByTtlTimeExpired(long time);


    long updateJobStatus(String jobId, byte status);



}
