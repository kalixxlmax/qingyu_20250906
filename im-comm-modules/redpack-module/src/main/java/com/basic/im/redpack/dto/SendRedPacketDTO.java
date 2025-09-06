package com.basic.im.redpack.dto;

import io.swagger.annotations.ApiModelProperty;
import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;

import java.io.Serializable;
import java.util.List;

/**
 * description: 发送红包数据类 <br>
 * date: 2020/2/29 0029  <br>
 * author: lidaye <br>
 * version: 1.0 <br>
 */
@Setter
@Getter
public class SendRedPacketDTO implements Serializable {


    //发送者用户Id
    @ApiModelProperty("用户编号")
    private int userId;
    @ApiModelProperty("发送到那个房间")
    private String roomJid;// 发送到那个房间
    @ApiModelProperty("发送给那个人")
    private @Indexed int toUserId;// 发送给那个人

    //红包发送者昵称
    @ApiModelProperty("红包发送者昵称")
    private String userName;
    //祝福语
    @ApiModelProperty("祝福语")
    private String greetings;
    //发送时间
    @ApiModelProperty("发送时间")
    private long sendTime;

    //红包类型

    /**
     *  1：普通红包  2：拼手气红包  3:口令红包  4 固定金额红包
     */
    @ApiModelProperty("红包类型")
    private  int type;
    //红包个数
    @ApiModelProperty("红包个数")
    private int count;

    //红包金额
    @ApiModelProperty("红包金额")
    private Double money;

    @ApiModelProperty("易宝红包Id")
    private String yopRedPacketId;

    /**
     * 4 固定金额红包  规定金额的列表 ,分割
     */
    @ApiModelProperty("红包规定金额列表")
    private String moneyListStr;


    private List<Double> moneyList;




}
