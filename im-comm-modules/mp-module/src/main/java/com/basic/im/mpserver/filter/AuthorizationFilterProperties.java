package com.basic.im.mpserver.filter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "authorizationfilter")
public class AuthorizationFilterProperties {

	private List<String> requestUriList;

	public List<String> getRequestUriList() {
		return requestUriList;
	}

	public void setRequestUriList(List<String> requestUriList) {
		this.requestUriList = requestUriList;
	}

}
