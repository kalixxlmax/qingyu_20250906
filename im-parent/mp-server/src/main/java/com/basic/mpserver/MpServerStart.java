package com.basic.mpserver;

import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.collect.Maps;
import com.basic.im.mpserver.filter.AuthorizationFilter;
import io.micrometer.core.instrument.MeterRegistry;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;



@Configuration
@EnableAutoConfiguration(exclude = { MongoAutoConfiguration.class, RedisAutoConfiguration.class,
		DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class
		})

@ComponentScan(basePackages = {"com.basic"})
@SpringBootApplication
public class MpServerStart extends SpringBootServletInitializer {
	
	private static final Logger log = LoggerFactory.getLogger(MpServerStart.class);
	
	public static void main(String[] args) {
		/**
		 * 内置Tomcat版本导致的 The valid characters are defined in RFC 7230 and RFC 3986 
		 * 修改 系统参数
		 */
		try {
			System.setProperty("tomcat.util.http.parser.HttpParser.requestTargetAllow","|{}");
			System.setProperty("rocketmq.client.logLevel", "WARN");
			SpringApplication.run(MpServerStart.class, args);
			
			 log.info(" Mp-Server start Success");
		} catch (Exception e) {
			log.error(" Mp-Server start Error",e);
		}

	}
	
	
	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		try {
			return application.sources(new Class[] { MpServerStart.class });
		} catch (Exception e) {
			log.error("启动报错",e);
			return null;
		}
		
	}


	/*@Bean
	public HttpMessageConverters customConverters() {
		return new HttpMessageConverters(
				new MappingFastjsonHttpMessageConverter());
	}*/

	@Bean
	public Jackson2ObjectMapperBuilderCustomizer jsonCustomer() {
		return jsonMapperBuilder -> {
			//添加Mongodb的ObjectId序列化的转换
			jsonMapperBuilder.serializerByType(ObjectId.class, new ToStringSerializer());
		};
	}

	@Autowired
	private AuthorizationFilter authorizationFilter;

	@Bean
	public FilterRegistrationBean<AuthorizationFilter>   filterRegistrationBean() {
		Map<String, String> initParameters = Maps.newHashMap();
		initParameters.put("enable", "true");
		List<String> urlPatterns = Arrays.asList("/*");

		FilterRegistrationBean<AuthorizationFilter> registrationBean = new FilterRegistrationBean<AuthorizationFilter>();
		registrationBean.setFilter(authorizationFilter);
		
		registrationBean.setInitParameters(initParameters);
		registrationBean.setUrlPatterns(urlPatterns);
		return registrationBean;
	}

	@Value("${spring.application.name}")
	private  String application;

	@Bean
	MeterRegistryCustomizer<MeterRegistry> configurer() {
		return (registry) -> registry.config().commonTags("application", application);
	}

}








