package com.basic.payment.dto;

import com.alibaba.fastjson.JSON;

public class BaseDTO  {

    @Override
    public String toString(){
       return JSON.toJSONString(this);
    }
}
