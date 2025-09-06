package com.basic.config;

import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.filter.FileInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfigurer implements WebMvcConfigurer {

    @Autowired
    private FileInterceptor fileInterceptor;

    private static final Logger log = LoggerFactory.getLogger(WebConfigurer.class);
    @Override
    public void addResourceHandlers(final ResourceHandlerRegistry registry) {
        String defaultPath = FileUtils.createPathSeparator(ConfigUtils.getSystemConfig().getBasePath());
        registry.addResourceHandler("/files/**").addResourceLocations("file:///" + defaultPath);
        log.info("File service access path:"+defaultPath);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(fileInterceptor).addPathPatterns("/upload/**");
    }
}