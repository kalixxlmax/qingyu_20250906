package com.basic.im.msg.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.msg.dao.FxSttingDao;
import com.basic.im.msg.entity.FxSetting;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/2 17:55
 */
@Repository
public class FxSettingDaoImpl extends MongoRepository<FxSetting, ObjectId> implements FxSttingDao {


    @Override
    public Class<FxSetting> getEntityClass() {
        return FxSetting.class;
    }

    @Override
    public void addMusicInfo(FxSetting musicInfo) {
        getDatastore().save(musicInfo);
    }

    @Override
    public void deleteMusicInfo(ObjectId id) {
       deleteById(id);
    }

    @Override
    public FxSetting getMusicInfoById(ObjectId id) {
        return get(id);
    }

    @Override
    public List<FxSetting> getMusicInfoList(int pageIndex, int pageSize) {
        Query query = createQuery();
        ascByquery(query,"sort");
        return queryListsByQuery(query,pageIndex,pageSize);
    }

    @Override
    public void updateMusicInfo(ObjectId id, Map<String, Object> map) {
        Query query = createQuery(id);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });

        update(query,ops);
    }

    @Override
    public PageResult<FxSetting> getMusicInfo(int pageIndex, int pageSize) {
        PageResult<FxSetting> result=new PageResult<>();
        Query query=createQuery();
        ascByquery(query,"sort");
        result.setData(queryListsByQuery(query,pageIndex,pageSize,1));
        return result;
    }
}
