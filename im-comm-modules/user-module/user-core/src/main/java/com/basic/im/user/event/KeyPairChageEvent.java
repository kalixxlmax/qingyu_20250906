package com.basic.im.user.event;

import lombok.Getter;
import lombok.Setter;

/**
 * KeyPairChageEvent <br>
 *
 * @date: 2020/10/15 0015  <br>
 * @author: lidaye <br>
 */
@Setter
@Getter
public class KeyPairChageEvent {

    public KeyPairChageEvent(int userId, String dhMsgPublicKey, String rsaMsgPublicKey) {
        this.userId = userId;
        this.dhMsgPublicKey = dhMsgPublicKey;
        this.rsaMsgPublicKey = rsaMsgPublicKey;
    }

    private int userId;

    /**
     * dh 消息公钥
     */
    private String dhMsgPublicKey;


    /**
     * rsa 消息公钥
     */
    private String rsaMsgPublicKey;
}
