package com.basic.im.msg.model;

import com.alibaba.fastjson.annotation.JSONField;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * 广场生活圈，模块查询条件模型 <br>
 *
 * @author: lidaye <br>
 * @date: 2021/12/2  <br>
 */

@ApiModel("广场生活圈，模块查询条件")
@Accessors(chain = true)
@Getter
@Setter
public class PublicMsgQueryModel {

    /**
     * 请求用户Id
     */
    private Integer requestUserId;

    @ApiModelProperty("页码")
    private Integer pageIndex=0;


    @ApiModelProperty("每页大小,默认10")
    private Integer pageSize=10;

    @ApiModelProperty("城市ID")
    private int cityId;

    @ApiModelProperty("标签")
    private String lable;

    @ApiModelProperty("关键词搜索")
    private String keyword;


    @ApiModelProperty("排序类型")
    private int sortType;

    //距离  km  公里  附件筛选条件
    //@ApiModelProperty("距离 km公里 筛选条件,默认100")
    private int distance=100;

    @ApiModelProperty("经度")
    private double longitude;


    @ApiModelProperty("纬度")
    private double latitude;





}
