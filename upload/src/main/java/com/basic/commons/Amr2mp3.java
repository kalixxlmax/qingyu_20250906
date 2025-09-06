package com.basic.commons;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FastDFSUtils;
import com.basic.commons.vo.FastDFSFile;

import it.sauronsoftware.jave.AudioUtils;

public class Amr2mp3 {
	public static void main(String[] args) throws Exception {
		String path1 = "C:\\Users\\Administrator\\Desktop\\test.amr";
		String path2 = "C:\\Users\\Administrator\\Desktop\\test.mp3";
		changeToMp3(path1, path2);
	}

	public static void changeToMp3(String sourcePath, String targetPath) {


		File source = new File(sourcePath);
		File target = new File(targetPath);
		//FileInputStream input = new FileInputStream(source);
	    AudioUtils.amrToMp3(source, target);

	}



	public static String changeToMp3AndUploadFastDFS(File sourceFile,File targetFile) throws IOException {

		AudioUtils.amrToMp3(sourceFile, targetFile);


		FileInputStream input = new FileInputStream(targetFile);
		byte[] byt = new byte[input.available()];
		input.read(byt);

		FastDFSFile file=new FastDFSFile();

		file.setName(targetFile.getName());
		file.setSize(targetFile.getTotalSpace());
		file.setContent(byt);
		String path=FastDFSUtils.uploadFile(file);


		//ResourcesDBUtils.saveFileUrl(2,path,-1);

		path=ConfigUtils.getFastDFSUrl(path);

		input.close();
		sourceFile.delete();
		targetFile.delete();
		return path;



	}


}
