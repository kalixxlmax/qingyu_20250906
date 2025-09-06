package com.basic.im.msg.dao.impl;

import com.basic.im.msg.dao.MsgPlayAmountDao;
import com.basic.im.msg.entity.PlayAmount;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/20 17:25
 */
@Repository
public class MsgPlayAmountDaoImpl extends MongoRepository<PlayAmount, ObjectId> implements MsgPlayAmountDao {

    @Override
    public Class<PlayAmount> getEntityClass() {
        return PlayAmount.class;
    }

    @Override
	public void addPlayAmount(PlayAmount playAmount) {

		// 持久化播放数量
		getDatastore().save(playAmount);
	}

    @Override
	public boolean exists(int userId, String msgId) {
		Query query =createQuery("msgId",msgId);
		addToQuery(query,"userId",userId);

		return exists(query);
	}

    @Override
	public List<PlayAmount> find(ObjectId msgId, ObjectId playAmountId, int pageIndex, int pageSize) {
		Query query=createQuery();
		if(null != playAmountId){
			addToQuery(query,"_id",playAmountId);
		}else{
			addToQuery(query,"msgId",msgId.toString());

		}
		descByquery(query,"time");
		return queryListsByQuery(query);
	}
}
