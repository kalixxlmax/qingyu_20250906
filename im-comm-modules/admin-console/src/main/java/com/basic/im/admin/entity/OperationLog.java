package com.basic.im.admin.entity;

import lombok.Data;

/**
 * @ClassName OperationLog
 * @Author xie yuan yang
 * @date 2020.10.28 17:03
 * @Description
 */
@Data
public class OperationLog {
    private Integer userId;
    private String url;
    private String ip;
    private long createTime;

    public OperationLog(Integer userId, String url, String ip, long createTime) {
        this.userId = userId;
        this.url = url;
        this.ip = ip;
        this.createTime = createTime;
    }
}
