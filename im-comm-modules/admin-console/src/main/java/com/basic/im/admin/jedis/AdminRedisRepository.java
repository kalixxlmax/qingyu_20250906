package com.basic.im.admin.jedis;

import com.basic.im.admin.entity.OperationLog;
import com.basic.im.comm.utils.DateUtil;
import com.basic.redisson.AbstractRedisson;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName AdminRedisRepository
 * @Author xie yuan yang
 * @date 2020.10.28 15:15
 * @Description
 */
@Slf4j
@Service(value = "adminRedisRepository")
public class AdminRedisRepository extends AbstractRedisson {

    @Autowired(required = false)
    private RedissonClient redissonClient;

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }

    /**
     * 记录后台登陆信息
     */
    public static final String OPERATION_LOG = "operation:log:%s:%s";


    /**
     * 保存登陆信息
     **/
    public void saveOperationLog(Integer userId, String url, String ip) {
        String key = String.format(OPERATION_LOG, String.valueOf(userId),DateUtil.currentTimeSeconds()+"_"+url);
        OperationLog operationLog = new OperationLog(userId,url,ip,DateUtil.currentTimeSeconds());
        setBucket(key, operationLog.toString());
    }

    /**
     * 获取个人登陆信息
     **/
    public List<OperationLog> getOperationLog(Integer userId){
        String key = String.format(OPERATION_LOG, String.valueOf(userId),"*");
        Iterable<String> keysByPattern = redissonClient.getKeys().getKeysByPattern(key);
        List<OperationLog> data = new ArrayList<>();
        keysByPattern.forEach(k->{
            data.add(getBucket(OperationLog.class, k));
        });
        return data;
    }

    /**
     * 获取全部登陆信息
     **/
    public List<Object> getAllLoginOperationLog(){
        String key = "operation:log:*";
        redissonClient.getKeys().getKeysByPattern(key);
        RList<Object> list = redissonClient.getList(key);
        return list.readAll();
    }

    /**
     * 全站公告
     */
    public static final String NOTICE_CONFIG = "notice:config";

    /**
     * 设置全站公告信息
     * @param jsonData 设置全站公告
     */
    public void saveNoticeConfig(String jsonData) {
        setBucket(NOTICE_CONFIG, jsonData);
    }

    /**
     * 获取全站公告信息
     **/
    public String getNoticeConfig(){
        RBucket<String> bucket = redissonClient.getBucket(NOTICE_CONFIG);
        return bucket.get();
    }

    /**
     * 每一次签到次数刷新时间大小，默认七天
     */
    public static String SIGN_MAX_COUNT_KEY = "sign_max_count";

    public void setSignMaxCount(Long maxCount) {
        redissonClient.getAtomicLong(SIGN_MAX_COUNT_KEY).set(maxCount);
    }

    public Long getSignMaxCount() {
        long maxCount = redissonClient.getAtomicLong(SIGN_MAX_COUNT_KEY).get();
        if (maxCount == 0) {
            return 7L;
        }
        return maxCount;
    }

    /**
     * 签到奖励策略，内容以 Map 存储，根据累计签到次数获取奖励金额，金额以分为单位
     */
    public static String SIGN_POLICY_AWARD_KEY = "sign_policy_award";

    public void setSignPolicyAward(Long day, Integer amount) {
        redissonClient.getMap(SIGN_POLICY_AWARD_KEY).put(day, amount);
    }

    public Map<Long, Integer> getSignPolicyAward() {
        RMap<Long, Integer> map = redissonClient.getMap(SIGN_POLICY_AWARD_KEY);
        if (map.size() == 0) {
            Map<Long,Integer> tempMap=new HashMap<>();
            for (long i = 1; i <= getSignMaxCount(); i++) {
                tempMap.put(i, i % 2 == 0 ? 0 : 1);
            }
            redissonClient.getMap(SIGN_POLICY_AWARD_KEY).putAll(tempMap);
            tempMap.clear();
            return tempMap;
        }
        return map;
    }

}
