package com.basic.im.company.service;

public interface SignManager {

    boolean saveUserSign(Integer userId, String signIp, String device, Long timeOut);
    void delUserSign(Integer userId, String signIp, String device);
}
