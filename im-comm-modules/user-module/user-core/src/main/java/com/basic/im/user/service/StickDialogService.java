package com.basic.im.user.service;

import com.basic.im.entity.StickDialog;

import java.util.List;

public interface StickDialogService {

    void add(StickDialog stickDialog);

    void delete(Integer userId,String jid);

    List<StickDialog> findListByUserId(Integer userId);

    void destroyUserRecord(Integer userId);
}
