package com.basic.service;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.basic.commons.vo.UploadFileModel;
import com.google.common.base.Joiner;
import com.basic.commons.Amr2mp3;
import com.basic.commons.utils.ConfigUtils;
import com.basic.commons.utils.FileUtils;
import com.basic.commons.utils.ResourcesDBUtils;
import com.basic.commons.vo.FileType;
import com.basic.commons.vo.UploadItem;
import com.basic.domain.ResourceFile;
import com.sun.org.apache.xalan.internal.res.XSLTErrorResources;
import net.coobird.thumbnailator.Thumbnails;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


/**
 * 第三方存储接口
 */
public interface ThirdUploadFileTemplateService extends UploadFileTemplateService {

    /**
     * 保存文件到第三方存储
     */
    String uploadThirdStorage(UploadFileModel model, String fileName, String contentType);

    String uploadThirdStorage(InputStream inputStream, String fileName, String contentType);

    String createThumbnailImage(String sourceImage, String targetImage);

    /**
     * 删除第三方文件
     */
    boolean deleteThirdFile(String url);

    /**
     * 创建第三方存储客户端
     */
    void createClient();

    /**
     * 关闭第三方存储客户端
     */
    void closeClient();


    default String imagePreCheck(UploadFileModel model, String fileName, String contentType) {

        MultipartFile multipartFile = model.getMultipartFile();
        // 是否开启色情图片过滤
        Integer openFilter = ConfigUtils.getSystemConfig().getOpenNsfwFilter();

        // 不开启 色情图片过滤
        if (openFilter.equals(0)) {
            return null;
        }

        // 要过滤图片的阈值,0-1之间,官方建议0.85以上算色情图片
        Double nsfwImagesScore = ConfigUtils.getSystemConfig().getNsfwImagesScore();

        // Python的图片鉴黄模块
        final String nsfwModuleUrl = ConfigUtils.getSystemConfig().getNsfwModuleUrl();

        String img=null;
        try {

            // 不能计算MD5值,否则图片的InputStream会丢失

            // 使用putObject上传一个文件到存储桶中
            // model.getInputStream() 图片存了是空白的,
            img=  uploadThirdStorage(multipartFile.getInputStream(), fileName, contentType);

            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {

                String projectPath = System.getProperty("user.dir");
                String forbid = projectPath + "/src/main/resources/forbid.png";
                File forbidFile = new File(forbid);

                try {

                    HashMap<String, Object> paramMap = new HashMap<>();
                    paramMap.put("file", com.basic.commons.utils.FileUtils.multipartFileToFile(multipartFile));

                    // 第一 ,异步鉴别涩图
                    String result = HttpUtil.post(nsfwModuleUrl, paramMap);
                    Double score = Double.parseDouble(result);

                    String fileMD5 = FileUtils.getFileMD5(multipartFile.getInputStream());

                    if (score > nsfwImagesScore) {
                        log.info("上传的图片涉黄,评分为:" + score);
                        InputStream input = null;
                        input = new FileInputStream(forbidFile);

                        // 第二,异步 违规图片替换
                        uploadThirdStorage(input, fileName, contentType);

                        // 第三,记录评分
                        // 保存分值

                        // TODO 用MD5,或者文件名来 查询;

                        // ResourcesDBUtils.updateImagesNSFW(imageURL+fileName, score);
                        ResourcesDBUtils.updateImagesNSFW(fileMD5, score);

                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

                return null;
            });


        } catch (Exception e) {
            e.printStackTrace();
        }


        return img;
    }

    /**
     * 具体业务操作实现
     */
    default UploadItem saveFile(UploadFileModel model) {
        String suffixName = FileUtils.getSuffixName(Objects.requireNonNull(model.getOriginalFilename()));
        String fileName = StringUtils.isEmpty(model.getFileName()) ? Joiner.on("").join(UUID.randomUUID().toString().replace("-", ""), ".", suffixName) : model.getFileName();
        FileType fileType = ConfigUtils.getFileType(suffixName);
        File[] uploadPath = model.getUploadPath();
        if (uploadPath == null) {
            uploadPath = ConfigUtils.getUploadPath(model.getUserId(), fileType);
        }
        String oUrl, tUrl = fileName;
        try {
            // 1、去除前缀路径
            fileName = formatUrl(uploadPath[0].getPath() + File.separator + fileName);

            if (FileType.Image == fileType && uploadPath.length == 2) { // 图片
                if (FileType.Image == fileType && uploadPath.length == 2) {
                    tUrl = formatUrl(uploadPath[1].getPath() + File.separator + tUrl);
                }
                oUrl = imagePreCheck(model, fileName, model.isTranPng() ? "image/png" : model.getContentType());
                if ("oss".equals(ConfigUtils.getSystemConfig().getUploadTypeName())) {
                    tUrl = createThumbnailImage(fileName, tUrl);
                } else {
                    tUrl = createThumbnailImage(model, tUrl, model.getContentType());
                }


                ResourcesDBUtils.saveFileUrl(1, oUrl, tUrl, model.getValidTime(), FileUtils.getFileMD5(model.getMultipartFile().getInputStream()), FileType.Image.getBaseName());
                return new UploadItem(model.getOriginalFilename(), oUrl, tUrl, (byte) 1, null, fileType);
            } else if (FileType.Image == fileType && uploadPath.length == 1) {
                oUrl = imagePreCheck(model, fileName, model.getContentType());
                ResourcesDBUtils.saveFileUrl(1, oUrl, 0, FileUtils.getFileMD5(model.getMultipartFile().getInputStream()), fileType.getBaseName());
                return new UploadItem(model.getOriginalFilename(), oUrl, null, (byte) 1, null, fileType);
            } else {
                UploadItem uploadItem;
                if ((fileName.contains(".amr")) && 1 == ConfigUtils.getSystemConfig().getAmr2mp3()) {

                    oUrl = imagePreCheck(model, fileName, "audio/mp3");
                    uploadItem = new UploadItem(model.getOriginalFilename(), oUrl, (byte) 1, null);
                } else {
                    oUrl = imagePreCheck(model, fileName, model.getContentType());
                    uploadItem = new UploadItem(model.getOriginalFilename(), oUrl, (byte) 1, null);
                }
                ResourcesDBUtils.saveFileUrl(1, oUrl, -1, FileUtils.getFileMD5(model.getMultipartFile().getInputStream()), fileType == null ? "" : fileType.getBaseName());
                uploadItem.setFileType(fileType);
                return uploadItem;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            /*try {
                if(null!=file&&null!=file.getInputStream()){
                    file.getInputStream().close();
                }
            }catch (IOException process){

            }*/

        }
        return null;
    }

    default String createThumbnailImage(UploadFileModel model, String fileName, String contentType) {
        String img=null;
        BufferedImage bufferedImage = null;
        try {
            String formatName = FileUtils.getSuffixName(fileName);
            if ("gif".equals(formatName) || "webp".equals(formatName)) {
                return imagePreCheck(model, fileName, contentType);
            } else {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                bufferedImage = ImageIO.read(model.getMultipartFile().getInputStream());
                Thumbnails.of(bufferedImage)
                        .scale(getScale(bufferedImage))
                        .outputFormat(formatName).toOutputStream(out);
                ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(out.toByteArray());
                img= uploadThirdStorage(arrayInputStream, fileName, contentType);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        // 如果缩略图也是 涉及 色情,则覆盖缩略图
        imagePreCheck(model, fileName, model.getContentType());

        return img;
    }


    default float getScale(BufferedImage sourceImg) {
        if (sourceImg == null) {
            return 0.1F;
        }
        int max = Math.max(sourceImg.getWidth(), sourceImg.getHeight());
        float scale = FileUtils.getThumbnailSize();
        float scaleF = 1.0F;
        if (max > scale) {
            scaleF = scale / max;
        }
        return scaleF;
    }


    /**
     * 重写父类方法，每次上传文件时创建客户端并且关闭
     */

    default UploadItem doSaveFileStore(UploadFileModel model) {
        try {
            this.createClient();
            return saveFile(model);
        } finally {
            closeClient();
        }
    }


    default UploadItem doSaveFileStoreInputStream(UploadFileModel model, int id, String fileName, File[] uploadPath, boolean isTranPng) {
        try {
            this.createClient();
            String path = imagePreCheck(model, fileName, "image/png");
            return new UploadItem(fileName, path, path, (byte) 1, null, FileType.Image);
        } finally {
            closeClient();
        }
    }

    /**
     * 删除文件
     */
    @Override
    default boolean deleteFile(ResourceFile resourceFile) {
        try {
            this.createClient();
            if (this.deleteThirdFile(resourceFile.getUrl())) {
                if (FileType.getFileType(resourceFile.getFileType()) == FileType.Image) {
                    this.deleteThirdFile(resourceFile.getMinUrl());
                }
                return true;
            }
            return false;
        } finally {
            closeClient();
        }
    }

    String regex = "/";

    default String formatUrl(String fileName) {
        if (!StringUtils.isEmpty(fileName)) {
            fileName = fileName.replaceAll("\\\\", regex);
            if (fileName.startsWith(ConfigUtils.getBasePath())) {
                fileName = fileName.replaceAll(ConfigUtils.getBasePath(), StrUtil.EMPTY);
                if (fileName.startsWith(regex)) {
                    fileName = fileName.replaceFirst(regex, StrUtil.EMPTY);
                }
            }
        }
        return fileName;
    }
}
