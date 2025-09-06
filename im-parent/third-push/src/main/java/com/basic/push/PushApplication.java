package com.basic.push;

import com.basic.im.push.autoconfigure.PushConfig;
import com.basic.im.push.service.PushServiceUtils;
import com.basic.im.user.service.UserCoreService;
import com.basic.push.prometheus.PrometheusDefaltExports;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

//@enablescheduling
@Configuration
@EnableAutoConfiguration(exclude = { MongoAutoConfiguration.class, RedisAutoConfiguration.class,
		DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class
		})
@ComponentScan(basePackages = {"com.basic"})
@SpringBootApplication  
public class PushApplication extends SpringBootServletInitializer implements CommandLineRunner {
	
	private static final Logger log = LoggerFactory.getLogger(PushApplication.class);
	
	public static void main(String... args) {
		/**
		 * 内置Tomcat版本导致的 The valid characters are defined in RFC 7230 and RFC 3986 
		 * 修改 系统参数
		 */
		try {
			System.setProperty("tomcat.util.http.parser.HttpParser.requestTargetAllow","|{}");
			System.setProperty("rocketmq.client.logLevel", "WARN");
			SpringApplication.run(PushApplication.class, args);
			
			 log.info("推送服务启动成功...=======>");
		} catch (Exception e) {
			log.error("启动报错",e);
		}
		
		  
	}
	

	

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		try {
			return application.sources(new Class[] { PushApplication.class });
		} catch (Exception e) {
			log.error("启动报错",e);
			return null;
		}
		
	}

	@Autowired(required=false)
	private PushConfig pushConfig;


	@Autowired(required=false)
	private UserCoreService userCoreService;


	@Override
	public void run(String... args) throws Exception {
		/*PushServiceUtils.setPushConfig(pushConfig);*/
		PushServiceUtils.setUserCoreService(userCoreService);
	}

	//配置注册表
	/**
	 * 使用HTTP Server暴露样本数据
	 **/
	@Value("${server.port}")
	private Integer port;

	@Value("${spring.application.name}")
	private String name;

	@Bean
	public MeterRegistry buildHTTPServer() {
		MeterRegistry meterRegistry = PrometheusDefaltExports.initialize(name, port);
		return meterRegistry;
	}

}
