package com.basic.im.user.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

@ApiModel("地理位置信息")
@Data
public class LocationExample {

	@ApiModelProperty("区县Id")
	protected Integer areaId = -1;

	@ApiModelProperty("城市Id")
	protected Integer cityId = -1;

	@ApiModelProperty("国家Id")
	protected Integer countryId = -1;

	@ApiModelProperty("省份Id")
	protected Integer provinceId = -1;

	@ApiModelProperty("纬度")
	protected double latitude = 0.0;

	@ApiModelProperty("经度")
	protected double longitude = 0.0;

}
