package com.basic.im.common.service;

import java.util.Map;

/**
 * @Description: TODO （红包操作接口 功能）
 * @Author xie yuan yang
 * @Date 2019/12/2
 **/
public interface RedPacketsManager {

    Object getRedPacketList(String userName, int pageIndex, int pageSize, String redPacketId,String status);

    Object receiveWater(String redId, int pageIndex, int pageSize);

    Object getRedPackListTimeOut(long outTime, int status);

    void updateRedPackListTimeOut(long outTime, int status, Map<String, Object> map);

    void autoRefreshRedPackect();

    String queryRedpackOverGroupCount(long startTime, long endTime);

    //计算红包金额
    Map<String,Object> calculateRedPacketMoney();

}
