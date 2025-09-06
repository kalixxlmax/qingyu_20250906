package com.basic.im.user.service.impl;

import com.basic.common.model.PageResult;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.HttpUtil;
import com.basic.im.user.dao.AllowRequestClientDao;
import com.basic.im.user.entity.AllowRequestClient;
import com.basic.im.user.service.AllowRequestClientService;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.DateUtil;
import org.bson.types.ObjectId;
import org.redisson.api.RList;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AllowRequestClientImpl <br>
 *
 * @author: lidaye <br>
 * @date: 2021/3/29 0029  <br>
 */
@Service
public class AllowRequestClientServiceImpl implements AllowRequestClientService {

    @Autowired
    private AllowRequestClientDao allowRequestClientDao;

    @Autowired
    private RedissonClient redissonClient;

    private final String ALLOW_REQUEST_IPLIST_KEY ="allow_request:iplist";

    @Override
    public PageResult<AllowRequestClient> queryList(int page, Integer limit, String keyword) {
        return allowRequestClientDao.queryList(page,limit,keyword);
    }

    @Override
    public void updateAllowRequest(AllowRequestClient requestClient) {
        if(!HttpUtil.isIPRight(requestClient.getIp())){
            throw new ServiceException("IP不合法");
        }
        if(null==requestClient.getStatus()){
            requestClient.setStatus((byte) KConstants.ONE);
        }
        requestClient.setModifTime(DateUtil.currentTimeSeconds());
        if(null==requestClient.getId()){
            requestClient.setId(ObjectId.get());
            requestClient.setCreateTime(DateUtil.currentTimeSeconds());
            allowRequestClientDao.saveEntity(requestClient);
            cleanRedisAllowRequest();
            return;
        }
        /*allowRequestClientDao.update(requestClient.getId(),requestClient);*/
        Update ops = new Update().set("id",requestClient.getId())
                .set("ip",requestClient.getIp())
                .set("status",requestClient.getStatus())
                .set("desc",requestClient.getDesc())
                .set("modifTime",requestClient.getModifTime());
        allowRequestClientDao.findAndModify(allowRequestClientDao.createQuery(requestClient.getId()),ops,AllowRequestClient.class);
        cleanRedisAllowRequest();


    }

    public void cleanRedisAllowRequest(){
        RSet<String> list = redissonClient.getSet(ALLOW_REQUEST_IPLIST_KEY);
        list.clear();
    }

    @Override
    public void deleteAllowRequest(ObjectId id) {
        allowRequestClientDao.deleteById(id);

        List<String> ipList = allowRequestClientDao.queryIpList();
        cleanRedisAllowRequest();

    }


    @Override
    public Set<String> queryIpList() {
        RSet<String> list = redissonClient.getSet(ALLOW_REQUEST_IPLIST_KEY);
        if(list.isEmpty()){
            List<String> ipList = allowRequestClientDao.queryIpList();
            list.addAll(ipList);
            return ipList.stream().collect(Collectors.toSet());
        }
        return list.readAll();


    }

    @Override
    public boolean isAllowRequest(String ip){
        Set<String> ipList = queryIpList();
        if(ipList.isEmpty()){
            return true;
        }
        return !ipList.contains(ip);
    }
}
