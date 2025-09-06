package com.basic.im.user.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.repository.MongoRepository;
import com.basic.im.user.dao.AllowRequestClientDao;
import com.basic.im.user.entity.AllowRequestClient;
import com.basic.im.utils.MongoUtil;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * AllowRequestClientDaoImpl <br>
 *
 * @author: lidaye <br>
 * @date: 2021/3/27 0027  <br>
 */
@Repository
public class AllowRequestClientDaoImpl extends MongoRepository<AllowRequestClient, ObjectId> implements AllowRequestClientDao {

    @Override
    public Class<AllowRequestClient> getEntityClass() {
        return AllowRequestClient.class;
    }


    @Override
    public PageResult<AllowRequestClient> queryList(int page, Integer limit, String keyword) {
        PageResult<AllowRequestClient> result = new PageResult<>();
        Query query = createQuery();
        if (!StringUtil.isEmpty(keyword)) {
            query.addCriteria(Criteria.where("ip").regex(MongoUtil.tranKeyWord(keyword)));
        }
        descByquery(query,"createTime");
        result.setCount(getDatastore().count(query,AllowRequestClient.class));
        query.with(createPageRequest(page, limit, 1));
        result.setData(getDatastore().find(query,AllowRequestClient.class));
        return result;
    }


    @Override
    public List<String> queryIpList(){
        return distinct("ip",createQuery("status",1),String.class);
    }
}
