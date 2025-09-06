package com.basic.im.dto;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.experimental.Accessors;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;

/**
 * 虚拟交易账单
 */
@Data
@Accessors(chain = true)
public class VirtualBillDTO {

    @ApiModelProperty("id")
    private @Id ObjectId id;

    @ApiModelProperty("用户id")
    private int userId;

    @ApiModelProperty("交易单号")
    private @Indexed String tradeNo;

    @ApiModelProperty("金额")
    private int amount;

    @ApiModelProperty("消费备注")
    private String desc;

    @ApiModelProperty("支付方式 1:支付宝支付 , 2:微信支付, 3:余额支付, 4:系统支付")
    private int payType;

    @ApiModelProperty("交易状态 0：创建  1：支付完成  2：交易完成  -1：交易关闭")
    private @Indexed int status;

    @ApiModelProperty("金额变更类型 1:收入 2:支出")
    private byte changeType;

    @ApiModelProperty("消费类型")
    private @Indexed int type;

    @ApiModelProperty("创建日期")
    private long createTime;

    @ApiModelProperty("是否有效：0 无效 1 有效")
    private byte isValid;

    @ApiModelProperty("修改时间")
    private byte modifyTime;
}
