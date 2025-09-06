package com.basic.im.mpserver.dao.impl;

import com.basic.im.mpserver.dao.MassContentDao;
import com.basic.im.mpserver.vo.MassContent;
import com.basic.im.repository.MongoRepository;
import com.basic.utils.DateUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Map;

/**
 * @Description 富文本群发相关操作
 * @Date 14:46 2020/12/14
 **/
@Repository
public class MassContentDaoImpl extends MongoRepository<MassContent,ObjectId> implements MassContentDao {

   
    @Override
    public Class<MassContent> getEntityClass() {
        return MassContent.class;
    }

    @Override
    public MassContent sava(MassContent massContent) {
        massContent.setCreateTime(DateUtil.currentTimeSeconds());
        return save(massContent);
    }

    @Override
    public MassContent find(ObjectId id) {
        return queryOneById(id);
    }


    @Override
    public void update(ObjectId id, Map<String, Object> map) {
        Query query = createQuery(id);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        update(query,ops);
    }

    @Override
    public void updateGiveLike(ObjectId id, int type) {
        Query query = createQuery(id);
        MassContent massContent = findOne(query);
        update(query,new Update().set("praise",massContent.getPraise()+type));
    }
}
