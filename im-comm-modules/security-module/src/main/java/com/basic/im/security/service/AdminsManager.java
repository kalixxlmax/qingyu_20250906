package com.basic.im.security.service;

import com.basic.im.entity.Config;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/4
 **/
public interface AdminsManager {

    Config getConfig();

    Config initConfig();
}
