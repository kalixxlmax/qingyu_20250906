package com.basic.mongodb.morphia;


import com.mongodb.*;
import com.basic.utils.StringUtil;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.annotations.Entity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;

@Configuration
@EnableConfigurationProperties(MongoConfig.class)
public class MorphiaAutoConfiguration {

	@Autowired
	private MongoConfig mongoConfig;

	public MongoClientOptions getMongoClientOptions(MongoConfig mongoConfig) {
			MongoClientOptions.Builder builder = MongoClientOptions.builder();
			builder.socketKeepAlive(true);
			builder.connectTimeout(mongoConfig.getConnectTimeout());
			builder.socketTimeout(mongoConfig.getSocketTimeout());
			builder.maxWaitTime(mongoConfig.getMaxWaitTime());
			builder.heartbeatFrequency(10000);// 心跳频率
			
			builder.readPreference(ReadPreference.nearest());
			 return builder.build();
	}

	@Bean(name = "morphia")
	public Morphia initMorphia() {
		Morphia morphia = new Morphia();
		if(!StringUtil.isEmpty(mongoConfig.getMapPackage()))
			morphia.mapPackage(mongoConfig.getMapPackage());
		return morphia;
	}

	
	@Bean(name = "mongoClient",destroyMethod = "close")
	public MongoClient initMongoClient()   {
		MongoClient mongoClient=null;
		try {
			MongoClientURI mongoClientURI=new MongoClientURI(mongoConfig.getUri());
			MongoCredential credential = null;
			 //是否配置了密码
			 if(!StringUtil.isEmpty(mongoConfig.getUsername())&&!StringUtil.isEmpty(mongoConfig.getPassword()))
				 credential = MongoCredential.createScramSha1Credential(mongoConfig.getUsername(), mongoConfig.getDbName(), 
						 mongoConfig.getPassword().toCharArray());
			 mongoClient = new MongoClient(mongoClientURI);
			return mongoClient;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Bean(name ="dsForRW")
	public Datastore dsForRW(MongoClient mongoClient,Morphia morphia){
		try {
			Datastore datastore = morphia.createDatastore(mongoClient, mongoConfig.getDbName());
			datastore.ensureIndexes();
			datastore.ensureCaps();
			return datastore;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	protected void mapPackage(String packageName, Morphia morphia) {
		try {
			String name = packageName.replace('.', '/');
			URL url = Thread.currentThread().getContextClassLoader().getResource(name);
			String[] names = url.toString().split("!");
			if (1 == names.length) {
				File packagePath = new File(names[0].replace("file:/", ""));
				if (packagePath.isDirectory()) {
					File[] files = packagePath.listFiles();
					for (File file : files) {
						if (file.getName().endsWith(".class") && -1 == file.getName().indexOf("$")) {
							String className = packageName + "."
									+ file.getName().replace('/', '.').replace(".class", "");
							Class<?> cls = Class.forName(className);
							if (null != cls.getAnnotation(Entity.class)) {
								morphia.map(Class.forName(className));
								System.out.println("mapPackage：" + className);
							}
						}
					}
				}
			} else if (3 == names.length) {
				String warName = names[0].replace("jar:file:", "");
				String jarName = names[1].substring(1);
				String packagePath = names[2].substring(1);

				// 获取war包对象
				JarFile jarFile = new JarFile(new File(warName));
				// 获取jar在war中的位置
				JarEntry ze = jarFile.getJarEntry(jarName);
				// 获取war包中的jar数据
				InputStream in = jarFile.getInputStream(ze);
				//
				JarInputStream is = new JarInputStream(in);
				JarEntry je;
				while (null != (je = is.getNextJarEntry())) {
					if (je.getName().startsWith(packagePath)) {
						if (je.getName().endsWith(".class") && -1 == je.getName().indexOf("$")) {
							String className = je.getName().replace('/', '.').replace(".class", "");
							Class<?> cls = Class.forName(className);
							if (null != cls.getAnnotation(Entity.class)) {
								morphia.map(Class.forName(className));
								System.out.println("mapPackage：" + className);
							}
						}
					}
				}
				is.closeEntry();
				is.close();
				jarFile.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
