package com.basic.im.admin.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.AreaConfigDao;
import com.basic.im.admin.entity.AreaConfig;
import com.basic.im.repository.MongoRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/9/3 15:33
 */
@Repository
public class AreaConfigDaoImpl extends MongoRepository<AreaConfig, ObjectId> implements AreaConfigDao {

    @Override
    public Class<AreaConfig> getEntityClass() {
        return AreaConfig.class;
    }

    @Override
    public void addAreaConfig(AreaConfig areaConfig) {
        getDatastore().save(areaConfig);
    }

    @Override
    public PageResult<AreaConfig> getAreaConfigList(String area, int pageIndex, int pageSize) {
       Query query=createQuery();
        if(!"".equals(area)){
            addToQuery(query,"area",area);
        }

        PageResult<AreaConfig> result=new PageResult<AreaConfig>();
        result.setCount(count(query));
        if (pageIndex < 1){
            query.with(createPageRequest(pageIndex,pageSize));
        }else{
            query.with(createPageRequest(pageIndex-1,pageSize));
        }
        result.setData(queryListsByQuery(query));
        return result;
    }

    @Override
    public void updateAreaConfig(ObjectId id, Map<String, Object> map) {
       Query query = createQuery("_id",id);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
        update(query,ops);
    }

    @Override
    public void deleteAreaConfig(ObjectId id) {
      deleteById(id);
    }
}
