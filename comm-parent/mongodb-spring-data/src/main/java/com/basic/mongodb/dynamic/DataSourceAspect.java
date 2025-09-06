package com.basic.mongodb.dynamic;


import com.basic.mongodb.dynamic.MongoContext;
import com.basic.mongodb.dynamic.OperatorMethod;
import com.basic.mongodb.dynamic.TargetDataSource;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@Aspect
@Component
@ConditionalOnProperty(name = "mongoconfig.separate", havingValue = "true")
public class DataSourceAspect {

    public DataSourceAspect() {
        System.err.println("start DataSourceAspect ");
    }

    @Pointcut("@annotation( TargetDataSource)")
    public void dataSourcePointCut() {
    }


    @Around("dataSourcePointCut()")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        Object result = null;
        try {
            MethodSignature signature = (MethodSignature) point.getSignature();
            Method method = signature.getMethod();
            TargetDataSource ds = method.getAnnotation(TargetDataSource.class);
            if (null == ds) {
                return point.proceed();
            }
            if (OperatorMethod.INSERT.equals(ds.operator()) || OperatorMethod.UPDATE.equals(ds.operator()) || OperatorMethod.DELETE.equals(ds.operator())) {
                result = point.proceed();
            } else if (OperatorMethod.SELECT.equals(ds.operator())) {
                result = point.proceed();
                if (null != result) {
                    return result;
                }
            }else if (OperatorMethod.COUNT.equals(ds.operator()) || OperatorMethod.EXISTS.equals(ds.operator())){
                MongoContext.setMongoDbFactory(ds.name());
                return point.proceed();
            }else {
                return point.proceed();
            }
            MongoContext.setMongoDbFactory(ds.name());
            return point.proceed();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            MongoContext.removeMongoDbFactory();
        }
        return result;
    }
}
