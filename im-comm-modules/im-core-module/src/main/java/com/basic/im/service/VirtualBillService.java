package com.basic.im.service;

import com.basic.common.model.PageResult;
import com.basic.im.dto.VirtualBillDTO;

/**
 * 虚拟账单服务层
 */
public interface VirtualBillService {
    boolean add(VirtualBillDTO virtualBillDTO);
    double getMoney(int userId);
    boolean depositMoney(int userId,double amount);
    void clearMoney(int userId);

    /**
     * @Description 查询 虚拟账单列表
     **/
    PageResult<VirtualBillDTO> findVirtualBillList(String startTime, String endTime, int page, int limit, String keyword, String payType, String status, String changeType, String isValid);
}
