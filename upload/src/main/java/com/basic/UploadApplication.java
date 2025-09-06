package com.basic;

import com.basic.commons.utils.*;
import com.basic.commons.vo.UploadItem;
import com.basic.domain.ResourceFile;
import com.basic.factory.UploadFileFactory;
import com.basic.task.TaskManager;
import org.apache.catalina.Context;
import org.apache.catalina.connector.Connector;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.tomcat.util.descriptor.web.SecurityCollection;
import org.apache.tomcat.util.descriptor.web.SecurityConstraint;
import org.csource.common.MyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author
 */
@ServletComponentScan
@EnableScheduling
@SpringBootApplication(exclude = {MongoAutoConfiguration.class})
public class UploadApplication extends SpringBootServletInitializer implements EnvironmentAware {

    private static final Logger log = LoggerFactory.getLogger(UploadApplication.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(UploadApplication.class);
        application.run(args);
        String configInfo = ConfigUtils.configInfo();
        log.info("upload config ======================>");
        log.info(configInfo);
        log.info("==========================================================================>");
        String dfsConfig = ConfigUtils.getFastDFSConfigInfo();
        log.info("FastDFS config ======================>");
        log.info(dfsConfig);
        initForBidImages();
        log.info("============================> 上传服务启动成功 请放心使用  ===========>");
        TaskManager taskManager = new TaskManager();
        taskManager.onStartup();
    }

    private static void initForBidImages() {

        String path=ConfigUtils.getBasePath();
        System.out.println(path);
        // extracted();
    }

    private static void extracted() {
        String projectPath = System.getProperty("user.dir");
        System.out.println(projectPath);

        String forbid = projectPath + "/src/main/resources/forbid.png";

        File file = new File(forbid);
        // // File file = new File("src/main/resources/forbid.png");
        Long time = System.currentTimeMillis();
        //
        MultipartFile fileToMultipartFile = fileToMultipartFile(file);
        //UploadItem uploadItem = UploadFileFactory.getUploadFileService(UploadFileFactory.LOCAL_TYPE).uploadFileStore(fileToMultipartFile, 1000, time);
    }


    public static MultipartFile fileToMultipartFile(File file) {
        FileItem fileItem = createFileItem(file);
        MultipartFile multipartFile = new CommonsMultipartFile(fileItem);
        return multipartFile;
    }

    // 导入的是org.apache.commons下的包，一定记住
    private static FileItem createFileItem(File file) {
        FileItemFactory factory = new DiskFileItemFactory(16, null);
        FileItem item = factory.createItem("textField", "text/plain", true, file.getName());
        int bytesRead = 0;
        byte[] buffer = new byte[8192];
        try {
            FileInputStream fis = new FileInputStream(file);
            OutputStream os = item.getOutputStream();
            while ((bytesRead = fis.read(buffer, 0, 8192)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            os.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return item;
    }

    @Override
    public void setEnvironment(Environment env) {
        try {
            FastDFSUtils.initServer(env);
        } catch (MyException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        // 注意这里要指向原先用 main 方法执行的Application启动类
        return builder.sources(UploadApplication.class);
    }


    //如果没有使用默认值80
    @Value("${http.port:8092}")
    Integer httpPort;

    //正常启用的https端口 如443
    @Value("${server.port}")
    Integer httpsPort;
    // http://192.168.0.90:8094/files/forbid.png

    @Bean
    @ConditionalOnProperty(name = "server.openHttps", havingValue = "true")
    public TomcatServletWebServerFactory servletContainer() {
        TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory() {
            @Override
            protected void postProcessContext(Context context) {
                SecurityConstraint securityConstraint = new SecurityConstraint();
                securityConstraint.setUserConstraint("CONFIDENTIAL");
                SecurityCollection collection = new SecurityCollection();
                collection.addPattern("/*");
                securityConstraint.addCollection(collection);
                context.addConstraint(securityConstraint);
            }
        };
        tomcat.addAdditionalTomcatConnectors(initiateHttpConnector());
        return tomcat;
    }

    private Connector initiateHttpConnector() {
        log.info("启用http转https协议，http端口：" + this.httpPort + "，https端口：" + this.httpsPort);
        Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
        connector.setScheme("http");
        connector.setPort(httpPort);
        connector.setSecure(true);
        connector.setRedirectPort(httpsPort);
        return connector;
    }



}
