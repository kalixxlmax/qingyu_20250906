package com.basic.im.user.service;

import com.basic.im.user.entity.User;
import com.basic.im.user.event.KeyPairChageEvent;
import com.basic.im.user.model.KSession;
import com.basic.im.user.model.UserExample;

public interface UserHandler {

    void registerToIM(String userId,String pwd);

    void registerHandler(String userId,String pwd,String nickname);

    int registerBeforeHandler(int userId,UserExample example);

    void registerAfterHandler(int userId,UserExample example,int codeUserId);

    void changePasswordHandler(User user, String oldPwd, String newPwd);

    void updateNickNameHandler(int userId,String oldNickName, String newNickName);

    void deleteUserHandler(int adminUserId,int userId);

    void userOnlineHandler(int userId);

    void refreshUserSessionHandler(int userId, KSession session);

    void clearUserSessionHandler(String accessToken);


    void addFriendsHandler(Integer userId,Integer toUserId);

    void publishEvent(Object event);

    void updateKeyPairHandler(KeyPairChageEvent keyPairChageEvent);
}
