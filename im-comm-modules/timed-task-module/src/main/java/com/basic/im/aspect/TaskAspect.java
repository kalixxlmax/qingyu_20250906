package com.basic.im.aspect;


import com.basic.im.comm.utils.DateUtil;
import com.basic.im.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
public class TaskAspect {

    @Autowired
    private TaskRepository commTaskRepository;

    @Pointcut(value = "execution(* com.basic.im.task..*.*(..))")
    public void logsPointCut() {}

    /**
     * 配置环绕通知
     */
    @Around("logsPointCut()")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        long beginTime = System.currentTimeMillis(); // 开始时间
        String className = point.getTarget().getClass().getName();
        String methodName = point.getSignature().getName();
        boolean isLock = false;
        String key = className + ":" +methodName;
        RLock lock = commTaskRepository.getLock(key);
        try{
            if (isLock=lock.tryLock()){
                Object result = point.proceed(); // 执行方法
                log.info("execute task success,class is {},method is {},executeTime is {},elapsed is {}",className,methodName, DateUtil.getFullString(),(System.currentTimeMillis() - beginTime));
                return result;
            }else{
                log.info("execute task not run,class is {},method is {}",className,methodName);
                return null;
            }
        }finally {
            if (lock!=null && isLock && lock.isLocked()){
                lock.unlock();
            }
        }
    }




}
