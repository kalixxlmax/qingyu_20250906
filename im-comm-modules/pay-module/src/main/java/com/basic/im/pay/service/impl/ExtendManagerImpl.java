package com.basic.im.pay.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUnit;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.pay.entity.ConsumeRecord;
import com.basic.im.repository.CoreRedisRepository;
import com.basic.im.user.constants.MoneyLogConstants;
import com.basic.im.user.dao.ExtendManagerDao;
import com.basic.im.user.dao.UserSignInfoManagerDao;
import com.basic.im.user.entity.UserMoneyLog;
import com.basic.im.user.entity.UserSign;
import com.basic.im.user.entity.UserSignInfo;
import com.basic.im.user.service.UserCoreService;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ExtendManagerImpl {

    @Autowired
    private ExtendManagerDao extendManagerDao;

    @Autowired
    private UserSignInfoManagerDao userSignInfoManagerDao;

    @Autowired
    private CoreRedisRepository coreRedisRepository;

    @Autowired
    private ConsumeRecordManagerImpl consumeRecordManager;

    @Autowired
    private UserCoreService userCoreService;




    private Float getMoney(Integer userId, String device, String IP, Integer day, Date now) throws ParseException {
        System.out.println(coreRedisRepository.getSignPolicyAward(Long.valueOf(day)));
        Float addMoney =  ((float)coreRedisRepository.getSignPolicyAward(Long.valueOf(day)) / 100);
        //记录用户签到的记录
        UserSign userSign = new UserSign();
        userSign.setCreateDate(now);
        userSign.setDevice(device);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        userSign.setSignDate(df.parse(df.format(now)));
        userSign.setSignIP(IP);
        userSign.setStatus(1);
        userSign.setId(new ObjectId());
        String signAward = "";
        if (addMoney == null) {
            if (signAward.length() == 0) {
                signAward = "money:0";
            } else {
                signAward = ",money:0";
            }
        } else {
            if (signAward.length() == 0) {
                signAward = "money:" + String.valueOf(addMoney);
            } else {
                signAward = ",money:" + String.valueOf(addMoney);
            }
        }
        userSign.setSignAward(signAward);
        userSign.setUpdateDate(new Date());
        userSign.setUserId(String.valueOf(userId));
        extendManagerDao.sava(userSign);
        return addMoney;
    }


    private int getSignDayInterval(Integer userId, Date now) throws ParseException {
        if(cn.hutool.core.date.DateUtil.between(new Date(),now, DateUnit.DAY) > 6){
            return 7;
        }
        List<UserSign> userSignByUserIdAndSignDate = extendManagerDao.getUserSignByUserIdAndSignDate(userId, now);
        if (CollectionUtil.isNotEmpty(userSignByUserIdAndSignDate)) {
            long between = cn.hutool.core.date.DateUtil.betweenDay(new Date(), userSignByUserIdAndSignDate.get(0).getSignDate(),true);
            return (int) between;
        } else {
            Date yesDate = getNestDayZeroTime(now, -1);
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            return getSignDayInterval(userId, format.parse(format.format(yesDate)));
        }
    }



    private Date getNestDayZeroTime(Date date, int days){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH)+days, 0, 0, 0);
        calendar.set(Calendar.MILLISECOND,0);
        Date newDate = calendar.getTime();
        return newDate;
    }


//



    public Float userSign(Integer userId, String device, String IP) throws ParseException {
        Date now = new Date();
        Float addMoney = null;
        UserSignInfo userSignInfo = null;
        //处理校验用户签到的信息
        List<UserSignInfo> signInfoList = userSignInfoManagerDao.getUserSignInfoByUserId(userId);
        if (CollectionUtil.isEmpty(signInfoList)) {
            addMoney = getMoney(userId, device, IP, 1, now);
            userSignInfo = new UserSignInfo();
            userSignInfo.setCreateDate(new Date());
            userSignInfo.setDialCount(0);
            userSignInfo.setSeriesSignCount(1);
            userSignInfo.setSignCount(1);
            userSignInfo.setSevenCount(1);
            userSignInfo.setStartSignDate(now);
            userSignInfo.setUpdateDate(new Date());
            userSignInfo.setUserId(String.valueOf(userId));
            userSignInfo.setId(new ObjectId());
            userSignInfoManagerDao.saveUserSignInfo(userSignInfo);
        } else {
            userSignInfo = signInfoList.get(0);
            if(userSignInfo.getStartSignDate() == null){
                addMoney = getMoney(userId, device, IP, 1, now);
                this.updateUserSignInfo(userSignInfo,1,1,1,0,now);
            }else{
                Date date = getNestDayZeroTime(now,0);
                SimpleDateFormat format =  new SimpleDateFormat( "yyyy-MM-dd" );
                int signDayInterval = getSignDayInterval(userId, format.parse(format.format(date)));

                if(signDayInterval == 7){
                    this.updateUserSignInfo(userSignInfo,  1, userSignInfo.getSignCount() + 1, 1, userSignInfo.getDialCount(), now);
                    addMoney = getMoney(userId, device, IP, 1, now);
                }else if(signDayInterval == 1 && userSignInfo.getSevenCount() == 7){
                    this.updateUserSignInfo(userSignInfo,  userSignInfo.getSeriesSignCount()+1, userSignInfo.getSignCount() + 1, 1, userSignInfo.getDialCount(), now);
                    addMoney = getMoney(userId, device, IP, 1, now);
                } else{
                    if(signDayInterval ==1){
                        if(userSignInfo.getSevenCount() == 6){
                            this.updateUserSignInfo(userSignInfo,userSignInfo.getSeriesSignCount()+1,userSignInfo.getSignCount()+1,userSignInfo.getSevenCount()+1,userSignInfo.getDialCount()+1,now);
                        }else{
                            this.updateUserSignInfo(userSignInfo,  userSignInfo.getSeriesSignCount()+1, userSignInfo.getSignCount() + 1, userSignInfo.getSevenCount()+1, userSignInfo.getDialCount(), now);
                        }

                        int s = userSignInfo.getSevenCount();
                        if(s%2 == 1){
                            addMoney = getMoney(userId, device, IP, s, now);
                        }else{
                            addMoney =  0f;
                            //记录用户签到的记录
                            UserSign userSign = new UserSign();
                            userSign.setCreateDate(now);
                            userSign.setDevice(device);
                            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                            userSign.setSignDate(df.parse(df.format(now)));
                            userSign.setSignIP(IP);
                            userSign.setStatus(1);
                            userSign.setId(new ObjectId());
                            String signAward = "";
                            if (addMoney == null) {
                                if (signAward.length() == 0) {
                                    signAward = "money:0";
                                } else {
                                    signAward = ",money:0";
                                }
                            } else {
                                if (signAward.length() == 0) {
                                    signAward = "money:" + String.valueOf(addMoney);
                                } else {
                                    signAward = ",money:" + String.valueOf(addMoney);
                                }
                            }
                            userSign.setSignAward(signAward);
                            userSign.setUpdateDate(new Date());
                            userSign.setUserId(String.valueOf(userId));
                            extendManagerDao.sava(userSign);
                        }

                    }else{
                        this.updateUserSignInfo(userSignInfo,  1, userSignInfo.getSignCount() + 1, 1, userSignInfo.getDialCount(), now);
                        addMoney = getMoney(userId, device, IP, 1, now);
                    }

                }
            }

        }

        //获取签到随机数字
        if (addMoney != null) {
            String tradeNo = StringUtil.getOutTradeNo();
            // 创建充值记录getUserStatusCount
            ConsumeRecord record = new ConsumeRecord();
            record.setUserId(userId);
            record.setTradeNo(tradeNo);
            record.setMoney(Double.valueOf(addMoney));
            record.setStatus(KConstants.OrderStatus.END);
            record.setType(KConstants.ConsumeType.USER_SIGN_PACKET);
            record.setPayType(KConstants.PayType.SYSTEMPAY); // type = 3 ：管理后台充值
            record.setDesc("sign packet");
            record.setTime(DateUtil.currentTimeSeconds());
            record.setChangeType((byte) 1);
            consumeRecordManager.saveConsumeRecord(record);
            try {
                //增加余额

                UserMoneyLog userMoneyLog =new UserMoneyLog(userId,0,tradeNo,Double.valueOf(addMoney),
                        MoneyLogConstants.MoenyAddEnum.MOENY_ADD, MoneyLogConstants.MoneyLogEnum.REDPACKET, MoneyLogConstants.MoneyLogTypeEnum.RECEIVE);
                Double resultDou = userCoreService.rechargeUserMoenyV1(userMoneyLog);
                return addMoney;
//                if (resultDou > 0) {
//                    return addMoney;
//                }
//                return -1f;
            } catch (Exception e) {
                return -1f;
            }
        }
        return 0f;
    }


    public void updateUserSignInfo(UserSignInfo userSignInfo, Integer seriesSignCount, Integer signCount, Integer sevenCount, Integer dialCount, Date startSignDate) {
        userSignInfo.setStartSignDate(startSignDate);
        userSignInfo.setSeriesSignCount(seriesSignCount);
        userSignInfo.setSevenCount(sevenCount);
        userSignInfo.setSignCount(signCount);
        userSignInfo.setDialCount(dialCount);
        userSignInfo.setUpdateDate(new Date());
        userSignInfoManagerDao.updateUserSignInfo(userSignInfo);
    }


    public Map<String, Object> getUserSignDateByWeek(Integer userId) throws ParseException{
        List<UserSignInfo> signInfoList = userSignInfoManagerDao.getUserSignInfoByUserId(userId);
        UserSignInfo userSignInfo = null;
        if (signInfoList != null && signInfoList.size() > 0){
            userSignInfo = signInfoList.get(0);
        }else {
            userSignInfo = new UserSignInfo();
            userSignInfo.setCreateDate(new Date());
            userSignInfo.setDialCount(0);
            userSignInfo.setSeriesSignCount(0);
            userSignInfo.setSignCount(0);
            userSignInfo.setSevenCount(0);
            userSignInfo.setStartSignDate(null);
            userSignInfo.setUpdateDate(new Date());
            userSignInfo.setUserId(String.valueOf(userId));
            userSignInfo.setId(new ObjectId());
            userSignInfoManagerDao.saveUserSignInfo(userSignInfo);
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("seriesSignCount",userSignInfo.getSeriesSignCount());
        resultMap.put("sevenCount",userSignInfo.getSevenCount());
        resultMap.put("signCount",userSignInfo.getSignCount());
        Date yesDate = getNestDayZeroTime(new Date(),0);
        SimpleDateFormat format =  new SimpleDateFormat( "yyyy-MM-dd" );
        List<UserSign> signList = extendManagerDao.getUserSignByUserIdAndSignDate(userId,format.parse(format.format(yesDate)));
        if (signList == null || signList.size() == 0){
            resultMap.put("signStatus", 2);
            Date oldDate = getNestDayZeroTime(new Date(),-1);
            SimpleDateFormat oldFormat =  new SimpleDateFormat( "yyyy-MM-dd" );
            List<UserSign> oldSignList = extendManagerDao.getUserSignByUserIdAndSignDate(userId,oldFormat.parse(oldFormat.format(oldDate)));
            //用户前一天没有签到
            if (oldSignList == null || oldSignList.size() == 0){

                UserSignInfo signInfo = new UserSignInfo();
                signInfo.setId(userSignInfo.getId());
                userSignInfoManagerDao.updateUserSignInfoCount(signInfo);
                resultMap.put("seriesSignCount",0);
                resultMap.put("sevenCount",0);
            }
        }else {
            resultMap.put("signStatus",1);
            resultMap.put("signAward",signList.get(0).getSignAward());
        }
        return resultMap;
    }

    public List<UserSign> getUserSignDateByMonth(Integer userId, String monthStr) throws ParseException {
        if (StringUtil.isEmpty(monthStr)){
            return new ArrayList<>();
        }
        Date month = DateUtil.toDate(monthStr);
        Calendar monthCal = Calendar.getInstance();
        monthCal.setTime(month);
        Date firstDay = getFirstDayOfMonth(monthCal.get(Calendar.YEAR),monthCal.get(Calendar.MONTH));
        Date lastDay = getLastDayOfMonth(monthCal.get(Calendar.YEAR),monthCal.get(Calendar.MONTH));
        SimpleDateFormat format =  new SimpleDateFormat( "yyyy-MM-dd" );
        List<UserSign> userSignList = extendManagerDao.findUserSignByMouth(userId,format.parse(format.format(firstDay)),format.parse(format.format(lastDay)));
        return userSignList;
    }


    private Date getFirstDayOfMonth(int year,int month){
        Calendar calendar = Calendar.getInstance();
        //设置年份
        calendar.set(Calendar.YEAR,year);
        //设置月份
        calendar.set(Calendar.MONTH, month);
        //获取某月最小天数
        int firstDay = calendar.getActualMinimum(Calendar.DAY_OF_MONTH);
        //设置日历中月份的最小天数
        calendar.set(Calendar.DAY_OF_MONTH, firstDay);
        return calendar.getTime();
    }

    private Date getLastDayOfMonth(int year,int month) {
        Calendar calendar = Calendar.getInstance();
        //设置年份
        calendar.set(Calendar.YEAR,year);
        //设置月份
        calendar.set(Calendar.MONTH, month);
        //获取某月最大天数
        int lastDay = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
        //设置日历中月份的最大天数
        calendar.set(Calendar.DAY_OF_MONTH, lastDay);
        return calendar.getTime();
    }
}
