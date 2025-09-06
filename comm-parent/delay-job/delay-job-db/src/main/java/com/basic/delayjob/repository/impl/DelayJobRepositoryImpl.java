package com.basic.delayjob.repository.impl;

import com.mongodb.client.result.DeleteResult;
import com.basic.delayjob.model.DelayJob;
import com.basic.delayjob.repository.DelayJobDO;
import com.basic.delayjob.repository.DelayJobRepository;
import com.basic.mongodb.springdata.BaseMongoRepository;
import com.basic.mongodb.utils.BeanCopyUtils;
import com.basic.utils.StringUtil;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class DelayJobRepositoryImpl extends BaseMongoRepository<DelayJobDO, String> implements DelayJobRepository {


    @Override
    public DelayJobDO getDelayJobByJobId(String jobId) {
        return queryOneById(jobId);
    }


    @Override
    public boolean addDelayJobToDB(DelayJob delayJob) {
        if(!exists("_id",delayJob.getJobId())){
            DelayJobDO delayJobDO=new DelayJobDO();
            BeanCopyUtils.copyProperties(delayJob,delayJobDO);
            save(delayJobDO);
            return true;
        }else {
            return false;
        }
    }


    @Override
    public long deleteDelayJob(String jobId) {
        DeleteResult deleteResult = deleteById(jobId);
        return deleteResult.getDeletedCount();
    }

    @Override
    public long deleteDelayJobAndTopic(String jobId,String topic) {

        Query query =createQuery(jobId);
        if(!StringUtil.isEmpty(topic)){
            addToQuery(query,"topic", topic);
        }

        DeleteResult deleteResult = deleteByQuery(query);

        return deleteResult.getDeletedCount();
    }


    @Override
    public List<DelayJobDO> queryListDelayJobByTopic(String topic) {
        Query query = createQuery("topic", topic);
        return queryListsByQuery(query);
    }

    @Override
    public List<DelayJobDO> queryListDelayJobByExecTimeExpired(long time) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("execTime").lte(time));

        return queryListsByQuery(query);
    }

    @Override
    public List<DelayJobDO> queryListDelayJobByTtlTimeExpired(long time) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("ttlTime").lte(time));

        return queryListsByQuery(query);
    }



    @Override
    public long deleteDelayJobByTopic(String topic) {
        Query query = createQuery("topic", topic);
        DeleteResult deleteResult = deleteByQuery(query);
        return deleteResult.getDeletedCount();
    }

    @Override
    public long deleteDelayJobByExecTimeExpired(long time) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("execTime").lt(time));
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
        Query query=createQuery(jobId);

        Update update=createUpdate();
        update.set("status",status);

        return update(query,update).getModifiedCount();
    }
}
