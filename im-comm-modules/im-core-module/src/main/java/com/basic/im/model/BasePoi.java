package com.basic.im.model;

import com.alibaba.fastjson.annotation.JSONField;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel("地址")
@Data
public class BasePoi {
    @ApiModelProperty("请求的用户Id")
    private Integer userId;

    @ApiModelProperty("地址")
    private String address;// 地址

    @ApiModelProperty("距离")
    private int distance;// 距离

    @JSONField(serialize = false)
    @ApiModelProperty("纬度")
    private double latitude;// 纬度

    @JSONField(serialize = false)
    @ApiModelProperty("经度")
    private double longitude;// 经度

    @JSONField(serialize = false)
    @ApiModelProperty("当前页")
    private int pageIndex = 0;

    @ApiModelProperty("当前页大小")
    @JSONField(serialize = false)
    private int pageSize = 10;

    @ApiModelProperty("索引")
    @JSONField(serialize = false)
    private int poiId;// 索引

    @ApiModelProperty("标签")
    private String tags;// 标签

    @ApiModelProperty("名称")
    private String title;// 名称
}
