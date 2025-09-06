package com.basic.im.common;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.config.AppConfig;
import com.basic.im.pay.dao.ConsumeRecordDao;
import com.basic.im.pay.entity.BaseConsumeRecord;
import com.basic.im.user.constants.MoneyLogConstants;
import com.basic.im.user.dao.MoneyLogDao;
import com.basic.im.user.entity.User;
import com.basic.im.user.entity.UserMoneyLog;
import com.basic.im.user.service.AuthKeysService;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.SpringBeansUtils;
import com.basic.utils.Base64;
import com.basic.utils.SnowflakeUtils;
import com.basic.utils.StringUtil;
import com.basic.utils.encrypt.AES;
import com.basic.utils.encrypt.RSA;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.Objects;
import java.util.function.Function;


/**
 * 用户金额操作工具类
 * @author huanghuijie
 */
@Slf4j
public class MoneyChangeUtil {

    private static UserCoreService userCoreService;
    private static ConsumeRecordDao consumeRecordDao;
    private static MoneyLogDao moneyLogDao;
    private static AuthKeysService authKeysService;

    private static String apiKey = null;
    private static AppConfig appConfig;

    static {
        userCoreService     = SpringBeansUtils.getBean(UserCoreService.class);
        consumeRecordDao    = SpringBeansUtils.getBean(ConsumeRecordDao.class);
        moneyLogDao         = SpringBeansUtils.getBean(MoneyLogDao.class);
        authKeysService     = SpringBeansUtils.getBean(AuthKeysService.class);
        appConfig           = SpringBeansUtils.getBean(AppConfig.class);
        apiKey              = appConfig.getApiKey();
    }

    /**
     * 金额增加
     */
    public static double addUserMoney(int userId, double amount, int payType,
                                      MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                      MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum, Function<BaseConsumeRecord, BaseConsumeRecord> function){
        return changeUserMoney(userId,0,amount,amount,payType, KConstants.OrderStatus.END,null, MoneyLogConstants.MoenyAddEnum.MOENY_ADD,moneyLogEnum,moneyLogTypeEnum,function);
    }
    public static double addUserMoney(int userId,double amount,int payType,
                                      MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                      MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum){
        return addUserMoney(userId,amount,payType, KConstants.OrderStatus.END,moneyLogEnum,moneyLogTypeEnum);
    }
    public static double addUserMoney(int userId,double amount,int payType, int orderStatus,
                                      MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                      MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum){
        return addUserMoney(userId,0,amount,payType,orderStatus,moneyLogEnum,moneyLogTypeEnum);
    }
    public static double addUserMoney(int userId, int toUserId, double amount,int payType, int orderStatus,
                                      MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                      MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum){
        return addUserMoney(userId,toUserId,amount,amount,payType,orderStatus,null,moneyLogEnum,moneyLogTypeEnum);
    }
    public static double addUserMoney(int userId, int toUserId, double amount, double operationAmount, int payType, int orderStatus,ObjectId targetId,
                                      MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                      MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum){
        return changeUserMoney(userId,toUserId,amount,operationAmount,payType,orderStatus,targetId, MoneyLogConstants.MoenyAddEnum.MOENY_ADD,moneyLogEnum,moneyLogTypeEnum);
    }


    /**
     * 金额减少
     */
    public static double reduceUserMoney(int userId, double amount, int payType,
                                         MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                         MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum,
                                         Function<BaseConsumeRecord, BaseConsumeRecord> function){
        return changeUserMoney(userId,0,amount,amount,payType, KConstants.OrderStatus.END,null, MoneyLogConstants.MoenyAddEnum.MOENY_REDUCE,moneyLogEnum,moneyLogTypeEnum,function);
    }
    public static double reduceUserMoney(int userId, double amount,int payType,
                                         MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                         MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum){
        return reduceUserMoney(userId,amount,payType, KConstants.OrderStatus.END,moneyLogEnum,moneyLogTypeEnum);
    }
    public static double reduceUserMoney(int userId, double amount,int payType, int orderStatus,
                                         MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                         MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum){
        return reduceUserMoney(userId,0,amount,payType,orderStatus,moneyLogEnum,moneyLogTypeEnum);
    }
    public static double reduceUserMoney(int userId, int toUserId, double amount,int payType, int orderStatus,
                                         MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                         MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum){
        return reduceUserMoney(userId,toUserId,amount,amount,payType,orderStatus,null,moneyLogEnum,moneyLogTypeEnum);
    }
    public static double reduceUserMoney(int userId, int toUserId, double amount, double operationAmount, int payType, int orderStatus,ObjectId targetId,
                                         MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                         MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum){
        return changeUserMoney(userId,toUserId,amount,operationAmount,payType,orderStatus, targetId, MoneyLogConstants.MoenyAddEnum.MOENY_REDUCE,moneyLogEnum,moneyLogTypeEnum);
    }


    /**
     * 修改用户金额
     * @param userId              用户id
     * @param toUserId            对方用户id
     * @param amount              金额
     * @param operationAmount     实际操作金额
     * @param payType             支付类型
     * @param orderStatus         订单状态
     * @param changEnum           修改类型
     * @param moneyLogEnum        业务类型
     * @param moneyLogTypeEnum    日志类型
     * @return                    当前余额
     */
    public static double changeUserMoney(int userId, int toUserId, double amount, double operationAmount, int payType, int orderStatus, ObjectId targetId,
                                         MoneyLogConstants.MoenyAddEnum changEnum,
                                         MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                         MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum){
        return changeUserMoney(userId, toUserId, amount, operationAmount, payType, orderStatus, targetId, changEnum, moneyLogEnum, moneyLogTypeEnum,null);
    }
    public static double changeUserMoney(int userId, int toUserId, double amount, double operationAmount, int payType, int orderStatus, ObjectId targetId,
                                         MoneyLogConstants.MoenyAddEnum changEnum,
                                         MoneyLogConstants.MoneyLogEnum moneyLogEnum,
                                         MoneyLogConstants.MoneyLogTypeEnum moneyLogTypeEnum, Function<BaseConsumeRecord, BaseConsumeRecord> function){
        if(amount>0.0){
            // 构建消费日志信息
            BaseConsumeRecord record=new BaseConsumeRecord();
            record.setUserId(userId);
            record.setToUserId(toUserId);
            record.setMoney(amount);
            record.setStatus(orderStatus);
            record.setPayType(payType);
            record.setTime(DateUtil.currentTimeSeconds());
            record.setOperationAmount(operationAmount);
            if (ObjectUtil.isNotNull(targetId)){
                record.setTargetId(targetId);
            }
            if (function!=null){
                record = function.apply(record);
            }
            if (StrUtil.isBlank(record.getTradeNo())){
                record.setTradeNo(SnowflakeUtils.getNextIdStr());
            }
            // 构建用户操作金额实体
            UserMoneyLog userMoneyLog =new UserMoneyLog(record.getUserId(),
                    record.getToUserId(),
                    record.getTradeNo(),
                    record.getMoney(), changEnum, moneyLogEnum, moneyLogTypeEnum);
            // 修改用户金额
            double balance = userCoreService.rechargeUserMoenyV1(userMoneyLog);
            // 设置不可被调用处更改的信息
            record.setType(moneyLogEnum.getType());
            record.setDesc(moneyLogEnum.getDesc());
            record.setChangeType(changEnum.getType());
            record.setCurrentBalance(balance);
            record.setBusinessId(userMoneyLog.getBusinessId());
            // 增加用户日志记录
            consumeRecordDao.addConsumRecord(record);
            log.info("changeUserMoney success,record is {}", JSONObject.toJSONString(record));
            return balance;
        }
        return userCoreService.getUserMoenyV1(userId);
    }


    /**
     * 根据账单统计用户余额
     * @param userId 用户id
     */
    public static double countUserMoney(Integer userId){
        return moneyLogDao.countUserMoney(userId);
    }

    /**
     * 获取用户余额
     */
    public static double getUserMoney(Integer userId){
        return userCoreService.getUserMoenyV1(userId);
    }



    /**
     * 用户余额检查，查询是否与账单保持一致
     * 只检查此功能开发完成后注册的用户
     * @return 余额正常返回 false，余额异常返回 true
     */
    public static boolean checkUserMoney(User user, double total){
        if (user==null){ return true; }
        try{
            if (user.getCreateTime() > 1596591655){
                return MoneyChangeUtil.countUserMoney(user.getUserId()) < total;
            }
            return false;
        }catch (Exception e){
            log.error("checkUserMoney failure,error message is {}",e.getMessage());
            return true;
        }
    }


    /**
     * 通用支付密码校验
     * @return 支付金额
     */
    public static double checkPassword(){
        ServletRequestAttributes requestAttributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = requestAttributes != null ? requestAttributes.getRequest() : null;
        VerifyUtil.isRollback(ObjectUtil.isNull(request),KConstants.ResultCode.PAY_FAILURE);

        // 获取支付所需的参数
        String  token  = request.getParameter("access_token");   // 用户TOKEN
        String  codeId = request.getParameter("codeId");         // CODE_ID
        String  data   = request.getParameter("data");           // 客户端加密字符串
        Integer userId = VerifyUtil.verifyUserId(ReqUtil.getUserId());

        // 验证支付密码
        String payPassword = authKeysService.getPayPassword(userId);
        VerifyUtil.isRollback(StrUtil.isBlank(payPassword),KConstants.ResultCode.PayPasswordNotExist);
        // 进行参数解码
        byte[] decode = getPayCodeById(userId, codeId);
        VerifyUtil.isRollback(ObjectUtil.isNull(decode),KConstants.ResultCode.PAY_FAILURE);
        JSONObject jsonObj = decodePayDataJson(data, decode);
        VerifyUtil.isRollback(ObjectUtil.isNull(jsonObj),KConstants.ResultCode.PAY_FAILURE);
        // 拼接加密字符串，对应客户端传递的加密数据
        StringBuffer macStrBuf = new StringBuffer();
        String moneyStr = Objects.requireNonNull(jsonObj).getString("money");
        macStrBuf.append(apiKey).append(userId).append(token).append(moneyStr);
        VerifyUtil.isRollback(ObjectUtil.isNull(checkAuthRSA(jsonObj, macStrBuf, payPassword, authKeysService.getPayPublicKey(userId))),KConstants.ResultCode.PAY_FAILURE);
        return Double.parseDouble(moneyStr);
    }

    public static byte[] getPayCodeById(int userId, String codeId) {
        String code = authKeysService.queryTransactionSignCode(userId, codeId);
        if (StringUtil.isEmpty(code)) {
            return null;
        }
        authKeysService.cleanTransactionSignCode(userId, codeId);
        return Base64.decode(code);
    }

    public static JSONObject decodePayDataJson(String data, byte[] decode) {
        String jsonStr;
        try {
            jsonStr = AES.decryptStringFromBase64(data, decode);
        } catch (Exception e) {
            return null;
        }
        JSONObject jsonObj = JSONObject.parseObject(jsonStr);
        String sign = jsonObj.getString("mac");
        if (StringUtil.isEmpty(sign)) {
            return null;
        }
        return jsonObj;
    }

    public static JSONObject checkAuthRSA(JSONObject jsonObj, StringBuffer macStrBuf, String payPwd, String publicKey) {
        String sign = jsonObj.getString("mac");
        macStrBuf.append(jsonObj.get("time")).append(payPwd);
        if (RSA.verifyFromBase64(macStrBuf.toString(), Base64.decode(publicKey), sign)) {
            return jsonObj;
        } else {
            return null;
        }
    }


}
