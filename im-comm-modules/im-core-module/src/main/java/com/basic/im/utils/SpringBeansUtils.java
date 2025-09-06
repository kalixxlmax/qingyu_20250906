package com.basic.im.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringBeansUtils implements ApplicationContextAware {

	private static ApplicationContext context;

	public static <T> T getBean(Class<?> requiredType) {
		return (T) context.getBean(requiredType);
	}

	public static <T> T getBeane(Class<?> requiredType) {
		return (T) context.getBean(requiredType);
	}

	public static ApplicationContext getContext() {
		return context;
	}

	@Override
	public void setApplicationContext(ApplicationContext ctx)
			throws BeansException {
		context = ctx;
	}

}
