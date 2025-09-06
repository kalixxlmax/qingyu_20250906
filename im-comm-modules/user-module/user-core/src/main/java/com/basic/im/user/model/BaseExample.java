package com.basic.im.user.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel("基本信息")
public class BaseExample {

	@ApiModelProperty("客户端使用的接口版本号")
	protected String apiVersion;

	@ApiModelProperty("客户端设备型号")
	protected String model;

	@ApiModelProperty("客户端设备操作系统版本号")
	protected String osVersion;

	@ApiModelProperty("客户端设备序列号")
	protected String serial;

	@ApiModelProperty("区县Id")
	protected Integer areaId;

	@ApiModelProperty("城市Id")
	protected Integer cityId;

	@ApiModelProperty("城市名称")
	protected String cityName;

	@ApiModelProperty("国家Id")
	protected Integer countryId;

	@ApiModelProperty("省份Id")
	protected Integer provinceId;

	@ApiModelProperty("详细地址")
	protected String address;

	@ApiModelProperty("位置描述")
	protected String location;

	@ApiModelProperty("纬度")
	protected double latitude;

	@ApiModelProperty("经度")
	protected double longitude;

	public String getApiVersion() {
		return apiVersion;
	}

	public void setApiVersion(String apiVersion) {
		this.apiVersion = apiVersion;
	}
	
	public String getModel() {
		return model;
	}

	public void setModel(String model) {
		this.model = model;
	}

	public String getOsVersion() {
		return osVersion;
	}

	public void setOsVersion(String osVersion) {
		this.osVersion = osVersion;
	}

	public String getSerial() {
		return serial;
	}

	public void setSerial(String serial) {
		this.serial = serial;
	}

	public Integer getAreaId() {
		return areaId;
	}

	public void setAreaId(Integer areaId) {
		this.areaId = areaId;
	}

	public Integer getCityId() {
		return cityId;
	}

	public void setCityId(Integer cityId) {
		this.cityId = cityId;
	}

	public String getCityName() {
		return cityName;
	}

	public void setCityName(String cityName) {
		this.cityName = cityName;
	}

	public Integer getCountryId() {
		return countryId;
	}

	public void setCountryId(Integer countryId) {
		this.countryId = countryId;
	}

	public Integer getProvinceId() {
		return provinceId;
	}

	public void setProvinceId(Integer provinceId) {
		this.provinceId = provinceId;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

}
