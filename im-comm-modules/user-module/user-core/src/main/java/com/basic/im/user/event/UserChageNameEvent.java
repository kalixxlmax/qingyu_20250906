package com.basic.im.user.event;

import lombok.Getter;
import lombok.Setter;

/**
 * 修改用户昵称事件
 */

@Getter
@Setter
public class UserChageNameEvent {


    public UserChageNameEvent(int userId,String oldNickName, String nickName) {
        this.userId = userId;
        this.oldNickName=oldNickName;
        this.nickName = nickName;
    }

    private int userId;

    private String oldNickName;

    private String nickName;




}
