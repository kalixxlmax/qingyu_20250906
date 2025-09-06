package com.basic.delayjob.repository.impl;

import com.mongodb.client.result.DeleteResult;
import com.basic.delayjob.model.DelayJob;
import com.basic.delayjob.model.WaitDelayJob;
import com.basic.delayjob.repository.DelayJobDO;
import com.basic.delayjob.repository.DelayJobRepository;
import com.basic.delayjob.repository.WaitDelayJobDO;
import com.basic.delayjob.repository.WaitDelayJobRepository;
import com.basic.mongodb.springdata.BaseMongoRepository;
import com.basic.mongodb.utils.BeanCopyUtils;
import com.basic.utils.StringUtil;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class WaitDelayJobRepositoryImpl extends BaseMongoRepository<WaitDelayJobDO, String> implements WaitDelayJobRepository {


    @Override
    public WaitDelayJobDO getDelayJobByJobId(String jobId) {
        return queryOne("jobId",jobId);
    }


    @Override
    public boolean addDelayJobToDB(WaitDelayJob delayJob) {
        if(!exists("jobId",delayJob.getJobId())){
            WaitDelayJobDO delayJobDO=new WaitDelayJobDO();
            BeanCopyUtils.copyProperties(delayJob,delayJobDO);
            save(delayJobDO);
            return true;
        }else {
            return false;
        }
    }


    @Override
    public long deleteDelayJob(String jobId) {
        DeleteResult deleteResult = deleteByAttribute("jobId",jobId);
        return deleteResult.getDeletedCount();
    }



    @Override
    public List<WaitDelayJobDO> queryListDelayJobByTopic(String topic) {
        Query query = createQuery("topic", topic);
        return queryListsByQuery(query);
    }

    @Override
    public List<WaitDelayJobDO> queryListDelayJobByExecTimeExpired(long time) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("execTime").lte(time));

        return queryListsByQuery(query);
    }

    @Override
    public List<WaitDelayJobDO> queryListDelayJobByWaitEndTime(long time) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("waitEndTime").lte(time));
        return queryListsByQuery(query);
    }

    @Override
    public List<WaitDelayJobDO> queryListDelayJobByTtlTimeExpired(long time) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("ttlTime").lte(time));

        return queryListsByQuery(query);
    }

    @Override
    public long deleteDelayJobAndTopic(String jobId,String topic) {

        Query query =createQuery("jobId",jobId);
        if(!StringUtil.isEmpty(topic)){
            addToQuery(query,"topic", topic);
        }

        DeleteResult deleteResult = deleteByQuery(query);

        return deleteResult.getDeletedCount();
    }

    @Override
    public long deleteDelayJobByTopic(String topic) {
        Query query = createQuery("topic", topic);
        DeleteResult deleteResult = deleteByQuery(query);
        return deleteResult.getDeletedCount();
    }

    @Override
    public long deleteDelayJobByWaitEndTimeExpired(long time) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("waitEndTime").lt(time));
        DeleteResult deleteResult = deleteByQuery(query);
        return deleteResult.getDeletedCount();
    }

    @Override
    public long deleteDelayJobByTtlTimeExpired(long time) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("ttlTime").lt(time));
        DeleteResult deleteResult = deleteByQuery(query);
        return deleteResult.getDeletedCount();
    }

    @Override
    public long updateJobStatus(String jobId, byte status) {
        Query query=createQuery("jobId",jobId);

        Update update=createUpdate();
        update.set("status",status);

        return update(query,update).getModifiedCount();
    }



}
