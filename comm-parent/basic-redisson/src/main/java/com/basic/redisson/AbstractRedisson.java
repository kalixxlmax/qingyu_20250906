package com.basic.redisson;

import com.basic.redisson.ex.LockFailException;
import com.basic.utils.StringUtil;
import org.redisson.api.*;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public abstract class AbstractRedisson {

        public abstract RedissonClient getRedissonClient();

        public String buildRedisKey(String keyFormat,Object ... params ){
            return String.format(keyFormat,params);
        }
        public <T> T getBucket(Class<T> tClass,String key){
            RBucket<T> bucket = getRedissonClient().getBucket(key);
            return bucket.get();
        }
        public <T> T getBucket(Class<T> tClass,String keyFormat,Object ... params ){
            String key=buildRedisKey(keyFormat,params);
            RBucket<T> bucket = getRedissonClient().getBucket(key);
            return bucket.get();
        }

        public <T> List<T> getList(Class<T> tClass,String key){
            RList<T> bucket = getRedissonClient().getList(key);
            return bucket.readAll();
        }
        public <T> List<T> getList(Class<T> tClass, String keyFormat, Object ... params ){
            String key=buildRedisKey(keyFormat,params);
            RList<T> bucket = getRedissonClient().getList(key);
            return bucket.readAll();
        }

    public boolean deleteBucket(String key){
        RBucket<Object> bucket = getRedissonClient().getBucket(key);
        return bucket.delete();
    }

     public boolean deleteBucket(String keyFormat,Object ... params ){
        String key=buildRedisKey(keyFormat,params);
        RBucket<Object> bucket = getRedissonClient().getBucket(key);
        return bucket.delete();
    }


    public boolean setBucket(String key,Object obj){
        RBucket<Object> bucket = getRedissonClient().getBucket(key);
        bucket.set(obj);
        return true;
    }

    public boolean setBucket(String key, Object obj, long time){
       return setBucket(key,obj,time,TimeUnit.SECONDS);
    }
    public boolean setBucket(String key, Object obj, long time, TimeUnit unit){
        RBucket<Object> bucket = getRedissonClient().getBucket(key);
        bucket.set(obj,time+getRandom(),unit);
        return true;
    }
    public void expire(RExpirable expirable, long time){
        expirable.expire(time+getRandom(),TimeUnit.SECONDS);
    }
    public boolean getLock(String lockKey,long time) {
        try {
            RLock lock = getRedissonClient().getLock(lockKey);

            return lock.tryLock(time,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }

    }
    public boolean getLock(String lockKey,long time,long timeout) {
        try {
             RLock lock = getRedissonClient().getLock(lockKey);

            return lock.tryLock(time,timeout,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

    }

    /**
     * 在redis分布式中执行回调函数
     * @param lockKey 分布式锁Key
     * @param time 获取锁时间 超时失败  秒
     * @param timeout  默认超时释放锁时间 秒
     * @param callBack 回调函数
     * @return
     * @throws LockFailException 获取锁失败异常
     * @throws InterruptedException
     */
    public Object executeOnLock(String lockKey,long time,long timeout,LockCallBack callBack) throws LockFailException,InterruptedException {
        RLock lock =getLock(lockKey);
        try {
           if(lock.tryLock(time,timeout,TimeUnit.SECONDS)){
               // 获取锁成功 执行回调
               try{
                   return callBack.execute(true);
               }finally {
                   if(lock.isLocked()) {
                       lock.unlock();
                   }
               }
           }else {
             throw new LockFailException("lock fail ");
           }
        } catch (InterruptedException e) {
            throw e;
        }
    }

    /**在redis分布式中执行回调函数
     * 默认 3秒获取锁失败
     * 默认 默认超时释放锁时间 30秒
     *
     * @param lockKey lockKey 分布式锁Key
     * @param callBack callBack 回调函数
     * @return
     * @throws LockFailException
     * @throws InterruptedException
     */
    public Object executeOnLock(String lockKey,LockCallBack callBack) throws LockFailException,InterruptedException {
        try {
            return executeOnLock(lockKey,3,30,callBack);
        } catch (InterruptedException e) {
            throw e;
        }

    }

    public RLock getLock(String key) {
        return getRedissonClient().getLock(key);
    }
    public RLock getLock(String keyFormat, Object ... params ) {
        String key=buildRedisKey(keyFormat,params);
        return getRedissonClient().getLock(key);
    }
    public boolean getLockResult(String keyFormat,long time,Object ... params ) {
        try {
            String key=buildRedisKey(keyFormat,params);
            RLock lock = getRedissonClient().getLock(key);

            return lock.tryLock(time,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

    }
    public boolean getLockResult(String keyFormat,long time,long timeout,Object ... params ) {
        try {
            String key=buildRedisKey(keyFormat,params);
            RLock lock = getRedissonClient().getLock(key);

            return lock.tryLock(time,timeout,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

    }
    public void unLock(String lockKey) {
            RLock lock = getRedissonClient().getLock(lockKey);
            lock.unlock();
    }


    public int getRandom(){
        return StringUtil.getRandom(120);
    }


    /**
     * 获取map对象
     *
     * @param <K>        the type parameter
     * @param <V>        the type parameter
     * @param objectName the object name
     * @return the r map
     */
    public  <K,V>RMap<K,V> getMap(String objectName){
        return getRedissonClient().getMap(objectName);
    }

    /**
     * 获取支持单个元素过期的map对象
     *
     * @param <K>        the type parameter
     * @param <V>        the type parameter
     * @param objectName the object name
     * @return the r map cache
     */
    public  <K,V> RMapCache<K,V> getMapCache(String objectName){
        return getRedissonClient().getMapCache(objectName);
    }

    /**
     * 获取set对象
     *
     * @param <V>        the type parameter
     * @param objectName the object name
     * @return the r set
     */
    public  <V> RSet<V> getSet(String objectName){
        return getRedissonClient().getSet(objectName);
    }

    /**
     * 获取SortedSet对象
     *
     * @param <V>        the type parameter
     * @param objectName the object name
     * @return the r sorted set
     */
    public  <V> RSortedSet<V> getSorteSet(String objectName){
        return getRedissonClient().getSortedSet(objectName);
    }

    /**
     * 获取ScoredSortedSett对象
     * @param objectName
     * @param <V>
     * @return
     */
    public  <V> RScoredSortedSet<V> getScoredSorteSet(String objectName) {
        return getRedissonClient().getScoredSortedSet(objectName);
    }


    /**
     * 获取list对象
     *
     * @param <V>        the type parameter
     * @param objectName the object name
     * @return the r list
     */
    public  <V> RList<V> getList(String objectName){
        return getRedissonClient().getList(objectName);
    }

    /**
     * 获取queue对象
     *
     * @param <V>        the type parameter
     * @param objectName the object name
     * @return the r queue
     */
    public  <V> RQueue<V> getQueue(String objectName){
        return getRedissonClient().getQueue(objectName);
    }


    /**
     * Get blocking queue r blocking queue.
     *
     * @param <V>        the type parameter
     * @param objectName the object name
     * @return the r blocking queue
     */
    public  <V> RBlockingQueue<V> getBlockingQueue(String objectName){
        return getRedissonClient().getBlockingQueue(objectName);
    }

    /**
     * Get atomic long r atomic long.
     *
     * @param objectName the object name
     * @return the r atomic long
     */
    public  RAtomicLong getAtomicLong(String objectName){
        return getRedissonClient().getAtomicLong(objectName);
    }



    /** @Description: redis 数据动态请求
     * @param key
     * @param pageIndex
     * @param pageSize
     * pageIndex = 0   pageSize = 10  0 - 10
     * pageIndex = 1   pageSize = 10  10 - 20
     * pageIndex = 2   pageSize = 10  20 - 30
     * @return
     **/
    public <T> List<T> redisPageLimit(String key,Integer pageIndex,Integer pageSize){
        RList<T> tList = getRedissonClient().getList(key);
        if(tList.size() == 0)
            return null;
        int fromIndex,toIndex = 0;
        fromIndex = pageIndex * pageSize;
        toIndex = (0 == pageIndex ? pageSize : (pageIndex + 1) * pageSize);
        int count = tList.size();
        if(toIndex >= count)
            toIndex = count;
        //log.info("======= fromIndex : "+ fromIndex +" ======= "+ "toIndex "+toIndex);
        if(fromIndex > toIndex)
            return null;
        RList<T> subList = tList.subList(fromIndex, toIndex);
        return subList.readAll();
    }
    /**
     * 分布式锁执行任务
     * @param lockKey
     * @param callable
     * @param <R>
     * @return
     * @throws Exception
     */
    public  <R> R callbackInLock(String lockKey, Callable<R> callable) throws Exception {
        RLock lock = getRedissonClient().getLock(lockKey);
        boolean tryLock=false;
        try {
            tryLock = lock.tryLock(3, 10, TimeUnit.SECONDS);
            if(!tryLock){
                throw new LockFailException("lock fail");
            }
            return callable.call();
        }catch (InterruptedException e){
            throw e;
        }finally {
            if(null!=lock&&tryLock){
                lock.unlock();
            }
        }


    }

    /**
     * 分布式锁执行任务
     * @param lockKey
     * @param callable
     * @param <R>
     * @return
     * @throws Exception
     */
    public  <R> R callbackInLock(String lockKey,long waitTime, long leaseTime, Callable<R> callable) throws Exception {
        RLock lock = getRedissonClient().getLock(lockKey);
        boolean tryLock=false;
        try {
            tryLock = lock.tryLock(waitTime, leaseTime, TimeUnit.SECONDS);
            if(!tryLock){
                throw new LockFailException("lock fail");
            }
            return callable.call();
        }catch (InterruptedException e){
            throw e;
        }finally {
            if(null!=lock&&tryLock){
                lock.unlock();
            }
        }


    }

    /**
     * 分布式锁执行任务
     * @param redissonClient
     * @param lockKey
     * @param callable
     * @param <R>
     * @return
     * @throws Exception
     */
    public static <R> R callbackInLock(RedissonClient redissonClient,String lockKey, Callable<R> callable) throws Exception {
        RLock lock = redissonClient.getLock(lockKey);
        boolean tryLock=false;
        try {
            tryLock = lock.tryLock(3, 10, TimeUnit.SECONDS);
            if(!tryLock){
                throw new LockFailException("lock fail");
            }
            return callable.call();
        }catch (InterruptedException e){
            throw e;
        }finally {
            if(null!=lock&&tryLock){
                lock.unlock();
            }
        }


    }

    /**
     * 分布式锁执行任务
     * @param redissonClient
     * @param lockKey
     * @param callable
     * @param <R>
     * @return
     * @throws Exception
     */
    public static <R> R callbackInLock(RedissonClient redissonClient,String lockKey,long waitTime, long leaseTime, Callable<R> callable) throws Exception {
        RLock lock = redissonClient.getLock(lockKey);
        boolean tryLock=false;
        try {
            tryLock = lock.tryLock(waitTime, leaseTime, TimeUnit.SECONDS);
            if(!tryLock){
                throw new LockFailException("lock fail");
            }
            return callable.call();
        }catch (InterruptedException e){
            throw e;
        }finally {
            if(null!=lock&&tryLock){
                lock.unlock();
            }
        }


    }


}
