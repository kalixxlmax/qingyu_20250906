package com.basic.commons.utils;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import cn.hutool.core.img.ImgUtil;
import com.madgag.gif.fmsware.AnimatedGifEncoder;
import com.madgag.gif.fmsware.GifDecoder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.basic.domain.ResourceFile;
import net.coobird.thumbnailator.Thumbnails;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mortennobel.imagescaling.AdvancedResizeOp;
import com.mortennobel.imagescaling.ResampleOp;
import com.basic.UploadApplication;
import com.basic.commons.vo.FileType;

import it.sauronsoftware.jave.AudioAttributes;
import it.sauronsoftware.jave.Encoder;
import it.sauronsoftware.jave.EncoderException;
import it.sauronsoftware.jave.EncodingAttributes;
import org.springframework.util.DigestUtils;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

public final class FileUtils {

    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    public static final String DEFAULT_SEPARATOR = "/";

    public static boolean deleteImage(String path, int isAvatar) {
        boolean result = false;
        //	1fc95d99277a47f5b76d9315b8be0897.jpg
        String fileName = null;
        //		d:/data/www/resources/u/6/3000000006/201608/
        String prefixPath = null;
        int fileNameIndex = 0;
        //d:/data/www/resources/u/6/3000000006/201608/o
        File oFile = null;
        //	//d:/data/www/resources/u/6/3000000006/201608/t
        File tFile;
        fileNameIndex = path.lastIndexOf("/") + 1;
        fileName = path.substring(fileNameIndex);
        if (1 != isAvatar) {
            prefixPath = path.substring(0, fileNameIndex - 2);
            oFile = new File(prefixPath + "o/" + fileName);
            tFile = new File(prefixPath + "t/" + fileName);
        } else {
            prefixPath = path.substring(0, fileNameIndex - 5);
            if (path.contains("/o/")) {
                oFile = new File(path);
                tFile = new File(path.replace("/o/", "/t/"));
            } else {
                tFile = new File(path);
                oFile = new File(path.replace("/t/", "/o/"));
            }
        }
        System.err.println(oFile.getPath());
        System.err.println(tFile.getPath());
        if (oFile.exists()) {
            result = oFile.delete();
            log.info("删除=====>" + oFile.getAbsolutePath() + "====>" + result);
        }
        if (tFile.exists()) {
            result = tFile.delete();
            log.info("删除=====>" + tFile.getAbsolutePath() + "====>" + result);
        }
        return result;
    }

    /**
     * @param @param  path  文件地址  url
     * @param @return 参数   文件的真实路径   不带 域名
     *                <p>
     *                示例
     *                <p>
     *                http://192.168.0.139/group1/M00/00/00/wKgAi1sEBvWAcwbaAAKOO3qxyM4141.png
     *                <p>
     *                返回  group1/M00/00/00/wKgAi1sEBvWAcwbaAAKOO3qxyM4141.png
     *                <p>
     *                /group1/M00/00/00/wKgAi1sEBvWAcwbaAAKOO3qxyM4141.png
     *                返回
     *                group1/M00/00/00/wKgAi1sEBvWAcwbaAAKOO3qxyM4141.png
     */
    public static String getAbsolutePath(String path) {
        String result = null;
        if (path.startsWith("http://") || path.startsWith("https://")) {
            String tempPath = path.substring(path.indexOf("//") + 2);
            result = tempPath.substring(tempPath.indexOf("/") + 1);
        } else if (path.startsWith("/")) {
            result = path.substring(path.indexOf("/") + 1);
        } else {
            result = path;
        }

        return result;
    }

    public static String readAll(InputStream in) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in,
                "UTF-8"));
        StringBuffer sb = new StringBuffer();
        String ln = null;
        while (null != (ln = reader.readLine())) {
            sb.append(ln);
        }
        return sb.toString();
    }

    public static String readAll(BufferedReader reader) {
        try {
            StringBuffer sb = new StringBuffer();
            String ln = null;
            while (null != (ln = reader.readLine())) {
                sb.append(ln);
            }
            return sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static float thumbnailSize = 0;

    public static float getThumbnailSize() {
        if (thumbnailSize < 100) {
            if(null!=ConfigUtils.getSystemConfig()) {
                thumbnailSize = ConfigUtils.getSystemConfig().getThumbnailSize();
            }
            if (thumbnailSize < 100) {
                thumbnailSize = 100;
            }
        }
        return thumbnailSize;
    }

	public static float getScale(BufferedImage sourceImg) {
    	if (sourceImg==null){return 0.1F;}
        int max = Math.max(sourceImg.getWidth(), sourceImg.getHeight());
        float scale = getThumbnailSize();
		float scaleF=1.0F;
		if(max>scale){
			scaleF=scale / max;
		}
        return scaleF;
    }

    public static ResampleOp createResampleOp(float scale, BufferedImage src) {
        int destWidth = (int) (src.getWidth() * scale);
        int destHeight = (int) (src.getHeight() * scale);
        if (2 > destHeight) {
            destHeight = 20;
        } else if (2 > destWidth) {
            destWidth = 20;
        }
        return new ResampleOp(destWidth, destHeight);
    }

    /**
     * 构建一个可复用的 InputStream
     */
    public static BufferedInputStream createBufferedStream(InputStream stream) {
        BufferedInputStream inputStream = new BufferedInputStream(stream);
        inputStream.mark(Integer.MAX_VALUE);
        return inputStream;
    }

    /**
     * 读取流到一个文件中
     */
    public static FileOutputStream readStreamFromInputStream(InputStream inputStream, File file) throws Exception {
        FileOutputStream out = new FileOutputStream(file, false);
        int length = 0;
        byte[] buf = new byte[1024];

        while ((length = inputStream.read(buf)) != -1) {
            out.write(buf, 0, length);
        }

        inputStream.reset();
        return out;
    }

    //传输压缩一个文件
    public static void transfer(InputStream stream, File oFile, File tFile, String formatName,boolean isTranPng) throws Exception{
        if (isTranPng && "png".equals(formatName)){
            transferFromPng(stream, oFile, tFile, formatName);
        }else{
            transfer(stream, oFile, tFile, formatName);
        }
    }
    @SuppressWarnings("resource")
    public static void transfer(InputStream stream, File oFile, File tFile, String formatName) throws Exception {
        BufferedInputStream inputStream = null;
        FileOutputStream outputStream = null;
        BufferedImage src = null;
        try {
            createParentPath(oFile);
            createParentPath(tFile);
            inputStream = createBufferedStream(stream);
            outputStream = readStreamFromInputStream(inputStream, oFile);
            try {
                src = ImageIO.read(inputStream);
                if (null == src) {
                    //直接复制不进行裁剪压缩
                    copyfile(tFile, oFile);
                    return;
                }
            } catch (Exception e) {
                System.out.println("FileUtils transfer ImageIO.read " + e.getMessage());
            }
            if ("gif".equals(formatName) || "webp".equals(formatName)) {
                copyfile(tFile, oFile);
               /* float scale=getScale(src);
				int destWidth = (int) (src.getWidth() * scale);
				int destHeight = (int) (src.getHeight() * scale);
				if(2>destHeight){
					destHeight=20;
				}else if(2>destWidth){
					destWidth=20;
				}
				zoomGifBySize(oFile.getAbsolutePath(),formatName,destWidth,destHeight,tFile.getAbsolutePath());*/
            }else if ("bmp".equals(formatName)){
                //ImgUtil.scale(oFile, tFile, getScale(src));   // 如果使用老版本文件裁剪，无法裁切 BMP ，需要另外使用新版本处理

                /**
                 * 新版压缩
                 */
                Thumbnails.of(oFile)
                        .scale(getScale(src))
                        .toFile(tFile);

            }else{
                // compressThumbnail(src, tFile, formatName); // 老版本文件裁剪
                //ImgUtil.scale(oFile, tFile, getScale(src));   // 新版本文件裁剪
                /**
                 * 新版压缩
                 */
                Thumbnails.of(oFile)
                        .scale(getScale(src))
                        .toFile(tFile);
            }
        } catch (Exception e) {
            log.error("transferFile failure，error message is {}",e.getMessage());
            log.error(e.getMessage(),e);
            throw e;
        } finally {
            if (null != stream) { stream.close(); }
            if (null != inputStream) { inputStream.close(); }
            if (null != outputStream) { outputStream.flush();outputStream.close(); }
        }
    }

    public static void compressThumbnail(BufferedImage src, File tFile, String formatName) throws IOException {
        FileOutputStream tOut = null;
        try {
            ResampleOp resampleOp = createResampleOp(getScale(src), src);
            resampleOp.setUnsharpenMask(AdvancedResizeOp.UnsharpenMask.VerySharp);
            BufferedImage dest = resampleOp.filter(src, null);
            tOut = new FileOutputStream(tFile, false);
            ImageIO.write(dest, formatName, tOut);
        } catch (Exception e) {
            throw e;
        } finally {
            if (null != tOut) { tOut.flush();tOut.close(); }
        }
    }

    //处理png 格式的图片  需要转成 jpg
    public static void transferFromPng(InputStream stream, File oFile, File tFile, String formatName) throws Exception {
        BufferedInputStream inputStream = null;
        FileOutputStream outputStream = null;
        BufferedImage src = null;
        try {
            inputStream = createBufferedStream(stream);
            outputStream = readStreamFromInputStream(inputStream, oFile);
            try {
                src = ImageIO.read(inputStream);
            } catch (Exception e) {
                log.info("FileUtils transfer ImageIO.read " + e.getMessage());
            }
            float scale = getScale(src);
            int destWidth = (int) (src.getWidth() * scale);
            int destHeight = (int) (src.getHeight() * scale);
            if (2 > destHeight) {
                destHeight = 20;
            } else if (2 > destWidth) {
                destWidth = 20;
            }
            BufferedImage to = new BufferedImage(destWidth, destHeight, BufferedImage.TYPE_INT_RGB);
            Graphics2D g2d = to.createGraphics();
            to = g2d.getDeviceConfiguration().createCompatibleImage(destWidth, destHeight, Transparency.TRANSLUCENT);
            g2d.dispose();
            g2d = to.createGraphics();

            Image from = src.getScaledInstance(destWidth, destHeight, src.SCALE_AREA_AVERAGING);
            g2d.drawImage(from, 0, 0, null);
            g2d.dispose();
            ImageIO.write(to, formatName, tFile);
        } catch (Exception e) {
            log.info(e.getMessage());
        } finally {
            if (null != stream) { stream.close(); }
            if (null != inputStream) { inputStream.close(); }
            if (null != outputStream) { outputStream.flush();outputStream.close(); }
        }
    }

    /**
     * @param imagePath  原图片路径地址，如：F:\\a.png
     * @param imgStyle   目标文件类型
     * @param width      目标文件宽
     * @param height     目标文件高
     * @param outputPath 输出文件路径（不带后缀），如：F:\\b，默认与原图片路径相同，为空时将会替代原文件
     * @throws IOException
     */
    public static void zoomGifBySize(String imagePath, String imgStyle, int width, int height, String outputPath) throws IOException {
        // 防止图片后缀与图片本身类型不一致的情况
        outputPath = outputPath + "." + imgStyle;
        // GIF需要特殊处理
        GifDecoder decoder = new GifDecoder();
        int status = decoder.read(imagePath);
        if (status != GifDecoder.STATUS_OK) {
            throw new IOException("read image " + imagePath + " error!");
        }
        // 拆分一帧一帧的压缩之后合成
        AnimatedGifEncoder encoder = new AnimatedGifEncoder();
        encoder.start(outputPath);
        encoder.setRepeat(decoder.getLoopCount());
        for (int i = 0; i < decoder.getFrameCount(); i++) {
            encoder.setDelay(decoder.getDelay(i));// 设置播放延迟时间
            BufferedImage bufferedImage = decoder.getFrame(i);// 获取每帧BufferedImage流
            BufferedImage zoomImage = new BufferedImage(width, height, bufferedImage.getType());
            Image image = bufferedImage.getScaledInstance(width, height, Image.SCALE_SMOOTH);
            Graphics gc = zoomImage.getGraphics();
            gc.setColor(Color.WHITE);
            gc.drawImage(image, 0, 0, null);
            encoder.addFrame(zoomImage);
        }
        encoder.finish();
        File outFile = new File(outputPath);
        BufferedImage image = ImageIO.read(outFile);
        ImageIO.write(image, outFile.getName(), outFile);
    }


    //传输压缩一两个文件
    @Deprecated
    public static void transferTwo(final InputStream inputStream, File oFile, final File tFile, final String formatName) throws Exception {
        final BufferedInputStream in = new BufferedInputStream(inputStream);
        in.mark(0);
        FileOutputStream out = new FileOutputStream(oFile, false);
		/*	int length = 0;
			byte[] buf = new byte[1024];

			while ((length = in.read(buf)) != -1) {
				out.write(buf, 0, length);
			}*/
        //in.reset();
        BufferedImage oSrc = ImageIO.read(in);
        in.reset();

        ResampleOp resampleOp = createResampleOp(getThumbnailSize(), oSrc);
        resampleOp.setUnsharpenMask(AdvancedResizeOp.UnsharpenMask.VerySharp);
        BufferedImage oImg = resampleOp.filter(oSrc, null);

        ImageIO.write(oImg, formatName, out);
        out.flush();
        out.close();

        new Thread(() -> {
            try {
                BufferedImage tSrc;
                tSrc = ImageIO.read(in);
                in.close();
                ResampleOp resampleOpT = createResampleOp(800f, tSrc);
                resampleOpT.setUnsharpenMask(AdvancedResizeOp.UnsharpenMask.VerySharp);
                BufferedImage tImg = resampleOpT.filter(tSrc, null);
                FileOutputStream tOut = new FileOutputStream(tFile, false);
                ImageIO.write(tImg, formatName, tOut);
                tOut.flush();
                tOut.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public static void transfer(InputStream in, File originalFile) throws Exception {
        createParentPath(originalFile);
        FileOutputStream out = new FileOutputStream(originalFile);
        try {
            int length = 0;
            byte[] buf = new byte[1024];

            while ((length = in.read(buf)) != -1) {
                out.write(buf, 0, length);
            }
        } catch (Exception e) {
            throw e;
        } finally {
			if (in != null) { in.close(); }
			out.flush();
			out.close();
		}


    }

    public static void copyfile(File file, File oldFile) throws Exception {
        FileOutputStream out = null;
        InputStream in = null;
        try {
            out = new FileOutputStream(file);
            in = new FileInputStream(oldFile);
            byte[] buffer = new byte[1024];
            int cnt;
            while ((cnt = in.read(buffer)) > 0) {
                out.write(buffer, 0, cnt);
            }
        } catch (Exception e) {
            log.error("copyfile failure,error message is {}",e.getMessage());
            throw e;
        } finally {
			if (in != null) { in.close(); }
			if (out != null) { out.flush(); out.close();}
		}

    }

    public static void changeToMp3(String sourcePath, String targetPath) {
        File source = new File(sourcePath);
        File target = new File(targetPath);
        AudioAttributes audio = new AudioAttributes();
        Encoder encoder = new Encoder();

        audio.setCodec("libmp3lame");
        EncodingAttributes attrs = new EncodingAttributes();
        attrs.setFormat("mp3");
        attrs.setAudioAttributes(audio);

        try {
            encoder.encode(source, target, attrs);
        } catch (IllegalArgumentException | EncoderException e) {
            e.printStackTrace();
        }
	}


    /**
     * 删除文件
     */
    public static boolean deleteFile(String childPath) {
        boolean result = false;
        String formatName = ConfigUtils.getFormatName(childPath);
        //.png
        FileType fileType = ConfigUtils.getFileType(formatName);
        String path = ConfigUtils.getBasePath() + childPath;
        String fileName = FileUtils.getFileName(childPath);
        FindIterable<Document> findIterable = ResourcesDBUtils.getFileByFileName(fileName);
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        if (mongoCursor.hasNext()) {
            int citations = Integer.parseInt(String.valueOf(mongoCursor.next().get("citations")));
            if (citations > 1) {
                result = ResourcesDBUtils.incCitationsByFileName(fileName, -1).getModifiedCount() > 0;
            } else {
                // 如果文件是图片
                if (FileType.Image == fileType) {
                    result = deleteImage(path, childPath.startsWith("/avatar") ? 1 : 0);
                } else {//如果文件不是图片
                    File file = new File(path);
                    if (file.exists()) {
                        result = file.delete();
                        log.info("删除=====>" + file.getAbsolutePath() + "====>" + result);
                    }
                    result = false;
                }
                ResourcesDBUtils.deleteFileByFileName(fileName);
            }
        }
        if (childPath.startsWith("/avatar")) {
            return deleteImage(path, childPath.startsWith("/avatar") ? 1 : 0);
        }else{
            // 不存在数据库并且文件存在直接删除
            File file=new File(path);
            if (file.exists()){
                return file.delete();
            }
        }
        return result;
    }

    public static String getFileName(String path) {
    	return path.substring(path.lastIndexOf("/") + 1);
    }


    /**
     * 判断父目录是否存在，如果不存在，则创建
     * @param file 文件流对象
     */
    public static void createParentPath(File file) {
        if (file == null) {
            throw new NullPointerException("file is null");
        }
        if (file.getParentFile() != null && !file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try { file.createNewFile(); } catch (IOException e) { e.printStackTrace(); }
    }

    /**
     * 判断是否以对应系统的文件夹分隔符结尾
     * @param path 要进行判断的路径
     */
    public static String createPathSeparator(String path) {
        if (path.endsWith(File.separator) || path.endsWith("/")) {
            return path;
        }
        return path + File.separator;
    }

    /**
     * 获取文件名后缀
     */
    public static String getSuffixName(String fileName) {
        int index = fileName.lastIndexOf('.');
        return -1 == index ? "" : fileName.substring(index + 1);
    }

    /**
     * 获取文件名前缀
     */
    public static String getPrefixName(String fileName) {
        int endIndex = fileName.indexOf('.');
        return -1 == endIndex ? fileName : fileName.substring(0, endIndex);
    }

    /**
     * 根据文件路径取得文件进行MD5校验
     */
    public static boolean checkFileMD5(String filePath,String md5Code){
        String fileMD5 = getFileMD5(filePath);
        return fileMD5.equals(md5Code);
    }

    /**
     * 根据文件流校验MD5
     */
    public static boolean checkFileMD5(FileInputStream fis,String md5Code){
        String fileMD5 = getFileMD5(fis);
        return fileMD5.equals(md5Code);
    }

    /**
     * 根据文件校验MD5
     */
    public static boolean checkFileMD5(File file,String md5Code){
        String fileMD5 = getFileMD5(file);
        return fileMD5.equals(md5Code);
    }

    /**
     * 通过文件路径获取文件MD5
     */
    public static String getFileMD5(String filePath){
        File file = new File(filePath);
        if (file.exists()){
            return getFileMD5(file);
        }
        log.error("getFileMD5 error file is notExists");
        return "";
    }

    /**
     * 通过文件获取MD5
     */
    public static String getFileMD5(File file){
        try {
            if (file!=null && file.exists()){
                return getFileMD5(new FileInputStream(file));
            }
        } catch (FileNotFoundException e) {
            log.error("file get MD5 error is {}",e.getMessage());
        }
        log.error("getFileMD5 error file is notExists");
        return "";
    }

    /**
     * 通过文件输出流获取文件MD5
     */
    public static String getFileMD5(InputStream fileInputStream){
        try{
            return DigestUtils.md5DigestAsHex(fileInputStream);
        } catch (IOException e) {
            log.error("file get MD5 error is {}",e.getMessage());
            return "";
        }
    }


    //  第一  File  ->  MultipartFile
    public static MultipartFile fileToMultipartFile(File file) {
        FileItem fileItem = createFileItem(file);
        MultipartFile multipartFile = new CommonsMultipartFile(fileItem);
        return multipartFile;
    }

    // 导入的是org.apache.commons下的包，一定记住
    public static FileItem createFileItem(File file) {
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


    //第二   MultipartFile    ->  File
    public static File multipartFileToFile(MultipartFile file) throws Exception {
        File toFile = null;
        if (file.equals("") || file.getSize() <= 0) {
            file = null;
        } else {
            InputStream ins = null;
            ins = file.getInputStream();
            toFile = new File(file.getOriginalFilename());
            inputStreamToFile(ins, toFile);
            ins.close();
        }
        return toFile;
    }

    //  MultipartFile    ->  File 获取流文件
    public static void inputStreamToFile(InputStream ins, File file) {
        try {
            OutputStream os = new FileOutputStream(file);
            int bytesRead = 0;
            byte[] buffer = new byte[8192];
            while ((bytesRead = ins.read(buffer, 0, 8192)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            os.close();
            ins.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //第三   InputStream -> File
    public static void copyInputStreamToFile(InputStream inputStream, File file)
            throws IOException {

        try (FileOutputStream outputStream = new FileOutputStream(file)) {

            int read;
            byte[] bytes = new byte[1024];

            while ((read = inputStream.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }

        }

    }

    public static InputStream  forbidImagesReplace() throws FileNotFoundException {
        String projectPath = System.getProperty("user.dir");
        String forbid = projectPath + "/src/main/resources/forbid.png";
        File file = new File(forbid);
        InputStream input = new FileInputStream(file);
        return input;
    }


    /**
     * 将InputStream写入本地文件
     * @param destination 写入本地目录
     * @param input	输入流
     * @throws IOException
     */
    private static void writeToLocal(String destination, InputStream input)
            throws IOException {
        int index;
        byte[] bytes = new byte[1024];
        FileOutputStream downloadFile = new FileOutputStream(destination);
        while ((index = input.read(bytes)) != -1) {
            downloadFile.write(bytes, 0, index);
            downloadFile.flush();
        }
        downloadFile.close();
        input.close();
    }

    /**
     * 将bytes写入本地文件
     * @param destination
     * @param bytes
     * @throws IOException
     */
    private static void writeToLocal(String destination, byte[] bytes)
            throws IOException {
        FileOutputStream downloadFile = new FileOutputStream(destination);
        downloadFile.write(bytes);
        downloadFile.flush();
        downloadFile.close();
    }


}
