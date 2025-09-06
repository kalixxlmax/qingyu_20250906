package com.basic.payment.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 下载对账文件请求
 */
@Data
@Accessors(chain = true)
public class OrderDownloadBillRequest {

    //对账日期
    private String billDate;

}
