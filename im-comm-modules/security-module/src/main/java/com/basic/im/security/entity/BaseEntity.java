package com.basic.im.security.entity;

import com.alibaba.fastjson.JSON;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.vo.JSONMessage;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @Description: TODO （公共实体类）
 * @Author xie yuan yang
 * @Date 2020/3/4
 **/

@Getter
@Setter
public class BaseEntity implements Serializable {

    private long createTime = DateUtil.currentTimeSeconds();

    private long modifyTime = DateUtil.currentTimeSeconds();

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
