package com.basic.im.room.vo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.bson.types.ObjectId;

/**
 * @author Administrator
 */
@ApiModel("群组付费参数")
@Data
public class RoomPayVO {
    @ApiModelProperty("群编号")
    private ObjectId roomId;
    @ApiModelProperty("金额")
    private int amount;
    @ApiModelProperty("天数")
    private int days;
}
