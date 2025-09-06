package com.basic.im.mpserver.dao.impl;

import com.basic.im.comm.utils.DateUtil;
import com.basic.im.mpserver.dao.MassCommentDao;
import com.basic.im.mpserver.vo.MassComment;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.*;

/**
 * @Description 富文本群发相关操作
 * @Date 14:46 2020/12/14
 **/
@Repository
public class MassCommentDaoImpl extends MongoRepository<MassComment,ObjectId> implements MassCommentDao {


    @Override
    public Class<MassComment> getEntityClass() {
        return MassComment.class;
    }

    @Override
    public MassComment sava(MassComment massComment) {
        massComment.setCreateTime(DateUtil.currentTimeSeconds());
        return save(massComment);
    }

    @Override
    public List<MassComment> find(String id,int pageIndex,int pageSize) {
        Query query = createQuery("massContentId", id);
        descByquery(query,"createTime");
        return queryListsByQuery(query,pageIndex,pageSize);
    }

    @Override
    public long countMassComment(String id) {
        Query query = createQuery("massContentId", id);
        return count(query);
    }

    @Override
    public void updateGiveLike(String commentId, int type) {
        Query query = createQuery(new ObjectId(commentId));
        MassComment massComment = findOne(query);
        update(query,new Update().set("praise",massComment.getPraise()+type));
    }
}
