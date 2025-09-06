package com.basic.im.mpserver.dao.impl;

import com.basic.im.comm.utils.DateUtil;
import com.basic.im.mpserver.dao.MassCommentPraiseDao;
import com.basic.im.mpserver.vo.MassCommentPraise;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

/**
 * @Description 富文本群发相关操作
 * @Date 14:46 2020/12/14
 **/
@Repository
public class MassCommentDaoPraiseImpl extends MongoRepository<MassCommentPraise,ObjectId> implements MassCommentPraiseDao {

    @Override
    public Class<MassCommentPraise> getEntityClass() {
        return MassCommentPraise.class;
    }

    @Override
    public MassCommentPraise sava(MassCommentPraise massCommentPraise) {
        massCommentPraise.setCreateTime(DateUtil.currentTimeSeconds());
        return save(massCommentPraise);
    }

    @Override
    public void delete(long userId, String contentId) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("userId").is(userId));
        query.addCriteria(Criteria.where("contentId").is(contentId));
        deleteByQuery(query);
    }

    @Override
    public MassCommentPraise findByUserId(long userId, String contentId) {
        Query query = createQuery();
        query.addCriteria(Criteria.where("userId").is(userId));
        query.addCriteria(Criteria.where("contentId").is(contentId));
        return findOne(query);
    }

}
