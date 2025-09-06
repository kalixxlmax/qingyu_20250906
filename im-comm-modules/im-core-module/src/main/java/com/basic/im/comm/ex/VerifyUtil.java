package com.basic.im.comm.ex;


import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.support.Callback;
import org.bson.types.ObjectId;
import org.springframework.util.Assert;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class VerifyUtil {

    @FunctionalInterface
    public interface ExecuteFunction {
        void execute();
    }

    /**
     * 根据条件决定是否执行
     */
    public static void execute(boolean isExecute, ExecuteFunction executeFunction){
        if (isExecute){
            executeFunction.execute();
        }
    }

    /**
     * 根据条件决定是否需要抛出异常
     */
    public static void isRollback(boolean isFailure, int code){
        if (isFailure){
            rollback(code);
        }
    }

    /**
     * 判断客户端传递参数是否为空，支持集合判断
     */
    public static <T>T isEmpty(T obj){
        isRollback(ObjectUtil.isEmpty(obj), KConstants.ResultCode.PARAM_IS_NOT_ALLOW_EMPTY);
        return obj;
    }

    public static void isEmpty(Object... objs){
        for (Object obj : objs) {
            isRollback(ObjectUtil.isEmpty(obj), KConstants.ResultCode.PARAM_IS_NOT_ALLOW_EMPTY);
        }
    }

    /**
     * 抛出异常
     */
    public static void rollback(int code){
        throw new ServiceException(code);
    }

    /**
     * 检测用户Id并且保证一定不为空
     * 如果为空或者不是纯数字，抛出异常
     */
    public static <T> T verifyUserId(T userId){
        isEmpty(userId);
        if (userId instanceof String){
            isRollback(NumberUtil.isNumber((CharSequence) userId),KConstants.ResultCode.UserNotExist);
        }else if (userId instanceof Integer){
            isRollback((Integer) userId==0,KConstants.ResultCode.UserNotExist);
        }
        return userId;
    }

    /**
     * 转换 ObjectId 时调用，避免空指针以及无效的 id 内容提示接口内部异常
     */
    public static ObjectId verifyObjectId(String objectId){
        return verifyObjectId(objectId,KConstants.ResultCode.ID_IS_INVALID);
    }

    public static ObjectId verifyObjectId(String objectId, Integer code){
        isRollback(StrUtil.isBlank(objectId) || !ObjectId.isValid(objectId),code);
        return new ObjectId(objectId);
    }

    /**
     * 验证是否为空并返回
     */
    public static <T> T verifyObject(T obj){
        isEmpty(obj);
        return obj;
    }
}
