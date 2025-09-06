package com.basic.im.manual.dao.impl;

import com.basic.common.model.PageResult;
import com.basic.im.comm.utils.NumberUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.manual.entity.Recharge;
import com.basic.im.repository.MongoRepository;
import com.basic.im.manual.dao.RechargeDao;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/12/2 15:49
 */
@Repository
public class RechargeDaoImpl extends MongoRepository<Recharge, ObjectId> implements RechargeDao {
    @Override
    public MongoTemplate getDatastore() {
        return super.getDatastore();
    }

    @Override
    public Class<Recharge> getEntityClass() {
        return Recharge.class;
    }

    @Override
    public void addRecharge(Recharge entity) {
        getDatastore().save(entity);
    }

    @Override
    public Recharge getRecharge(ObjectId id) {
        return get(id);
    }

    @Override
    public PageResult<Recharge> getRechargeList(int pageIndex, int pageSize, String keyword, long startTime, long endTime) {
        PageResult<Recharge> result = new PageResult<>();
        Query query = createQuery();

        query.with(createPageRequest(pageIndex,pageSize));

        if(!StringUtil.isEmpty(keyword)){
            query.addCriteria(Criteria.where("userId").is(Integer.valueOf(keyword)));
        }
        //时间范围查询
        if(0!=startTime && 0!=endTime) {
            query.addCriteria(Criteria.where("createTime").gt(startTime).lte(endTime));
        }

        descByquery(query,"createTime");
        result.setData(getDatastore().find(query,getEntityClass()));
        result.setCount(count(query));
        return result;
    }

    @Override
    public Map<String,Object> queryRecharge(int pageIndex, int pageSize,String keyword,long startTime,long endTime){
        Map<String,Object> map = new HashMap<>(4);
        Criteria criteria = createCriteria();

        criteria.and("status").ne(0);
        if(!StringUtil.isEmpty(keyword)&& NumberUtil.isNum(keyword)){
            criteria.and("userId").is(Integer.valueOf(keyword));
        }
        Criteria timeCriteria = Criteria.where("createTime");
        if(0<startTime){
            timeCriteria.gt(startTime);
        }
        if(0<endTime){
            timeCriteria.lt(endTime);
        }
        if(0<startTime||0<endTime){
            criteria.andOperator(timeCriteria);
        }


        GroupOperation groupOperation = Aggregation.group("status").sum("money").as("total");

        Aggregation aggregation = Aggregation.newAggregation(Aggregation.match(criteria),
                Aggregation.sort(Sort.Direction.DESC,"createTime"),
                Aggregation.skip(pageIndex*pageSize),
                Aggregation.limit(pageSize),
                groupOperation,
                Aggregation.project("status","total")

        );

        AggregationResults<Document> aggregate = getDatastore().aggregate(aggregation, Recharge.class, Document.class);
        Iterator<Document> iterator = aggregate.iterator();
        Document dbObject=null;
        // 总充值申请 充值成功 充值失败 申请中
        double totalRecharge = 0, successRecharge = 0, failureRecharge = 0, applyRecharge = 0,ignoreCount=0;
        while (iterator.hasNext()){
            dbObject = iterator.next();
            if(null==dbObject){
                continue;
            }

            if(dbObject.get("_id").equals(1)){
                applyRecharge = (double)dbObject.get("total");
            }else if(dbObject.get("_id").equals(2)){
                successRecharge = (double)dbObject.get("total");
            }else if(dbObject.get("_id").equals(-1)){
                failureRecharge = (double)dbObject.get("total");
            }else if(dbObject.get("_id").equals(-2)){
                ignoreCount = (double)dbObject.get("total");
            }
            totalRecharge = applyRecharge+successRecharge+failureRecharge;
        }

        map.put("totalRecharge",totalRecharge);
        map.put("successRecharge",successRecharge);
        map.put("failureRecharge",failureRecharge);
        map.put("applyRecharge",applyRecharge);
        return map;
    }

    @Override
    public Recharge updateRecharge(ObjectId id, Map<String, Object> map) {
        Query query = createQuery("_id",id);
        Update ops = createUpdate();
        map.forEach((key,value)->{
            ops.set(key,value);
        });
       return getDatastore().findAndModify(query,ops,getEntityClass());
    }

    @Override
    public void deleteRecharge(ObjectId id) {
        deleteById(id);
    }
}
