package com.basic.commons.utils;

import java.awt.Image;
import java.awt.Toolkit;
import java.awt.image.BufferedImage;
import java.awt.image.MemoryImageSource;
import java.io.*;

import javax.imageio.ImageIO;

import cn.hutool.core.img.ImgUtil;
import com.jhlabs.image.ImageUtils;
// 移除已废弃的JPEG编码器导入
import net.coobird.thumbnailator.Thumbnails;

public class ThumUtils {

	private static final String BMP = "bmp";
	private static final String PNG = "png";
	private static final String GIF = "gif";
	private static final String JPEG = "jpeg";
	private static final String JPG = "jpg";

	public static void getThumbnail(String source, String output, int width, int height) {
		boolean adjustSize = true;
		if (source == null || source.equals("") || width < 1 || height < 1) {
			return;
		}
		if (output == null || output.equals("")) {
			return;
		}
		source = source.toLowerCase();
		if (source.endsWith(BMP)) {
			BMPThumbnailHandler(source, output, width, height, adjustSize);
		} else if (source.endsWith(PNG) || source.endsWith(GIF) || source.endsWith(JPEG) || source.endsWith(JPG)) {
			thumbnailHandler(source, output, width, height, adjustSize);
		}
	}

	private static void thumbnailHandler(String source, String output, int width, int height, boolean adjustSize) {
		try {
			File sourceFile = new File(source);
			if (sourceFile.exists()) {
				Image image = ImageIO.read(sourceFile);
				int theImgWidth = image.getWidth(null);
				int theImgHeight = image.getHeight(null);
				int[] size = { theImgWidth, theImgHeight };
				if (adjustSize) {
					size = adjustImageSize(theImgWidth, theImgHeight, width, height);
				}
				StringBuffer thumbnailFile = new StringBuffer();
				// thumbnailFile.append(sourceFile.getParent());
				// thumbnailFile.append(File.separatorChar);
				thumbnailFile.append(output);
				writeFile(image, size[0], size[1], thumbnailFile.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	private static void BMPThumbnailHandler(String source, String output, int width, int height, boolean adjustSize) {
		try {
			File sourceFile = new File(source);
			if (sourceFile.exists()) {
				Image image = getBMPImage(source);
				int theImgWidth = image.getWidth(null);
				int theImgHeight = image.getHeight(null);
				int[] size = { theImgWidth, theImgHeight };
				if (adjustSize) {
					size = adjustImageSize(theImgWidth, theImgHeight, width, height);
				}
				StringBuffer thumbnailFile = new StringBuffer();
				// thumbnailFile.append(sourceFile.getParent());
				// thumbnailFile.append(File.separatorChar);
				thumbnailFile.append(output);
				writeFile(image, size[0], size[1], thumbnailFile.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	private static Image getBMPImage(String source) throws Exception {

		FileInputStream fs = null;
		Image image = null;
		try {
			fs = new FileInputStream(source);
			int bfLen = 14;
			byte bf[] = new byte[bfLen];
			fs.read(bf, 0, bfLen); // 读取14字节BMP文件头
			int biLen = 40;
			byte bi[] = new byte[biLen];
			fs.read(bi, 0, biLen); // 读取40字节BMP信息头

			// 源图宽度
			int nWidth = (((int) bi[7] & 0xff) << 24) | (((int) bi[6] & 0xff) << 16) | (((int) bi[5] & 0xff) << 8) | (int) bi[4] & 0xff;

			// 源图高度
			int nHeight = (((int) bi[11] & 0xff) << 24) | (((int) bi[10] & 0xff) << 16) | (((int) bi[9] & 0xff) << 8) | (int) bi[8] & 0xff;

			// 位数
			int nBitCount = (((int) bi[15] & 0xff) << 8) | (int) bi[14] & 0xff;

			// 源图大小
			int nSizeImage = (((int) bi[23] & 0xff) << 24) | (((int) bi[22] & 0xff) << 16) | (((int) bi[21] & 0xff) << 8) | (int) bi[20] & 0xff;

			// 对24位BMP进行解析
			if (nBitCount == 24) {
				int nPad = (nSizeImage / nHeight) - nWidth * 3;
				int nData[] = new int[nHeight * nWidth];
				byte bRGB[] = new byte[(nWidth + nPad) * 3 * nHeight];
				fs.read(bRGB, 0, (nWidth + nPad) * 3 * nHeight);
				int nIndex = 0;
				for (int j = 0; j < nHeight; j++) {
					for (int i = 0; i < nWidth; i++) {
						nData[nWidth * (nHeight - j - 1) + i] = (255 & 0xff) << 24 | (((int) bRGB[nIndex + 2] & 0xff) << 16)
								| (((int) bRGB[nIndex + 1] & 0xff) << 8) | (int) bRGB[nIndex] & 0xff;
						nIndex += 3;
					}
					nIndex += nPad;
				}
				Toolkit kit = Toolkit.getDefaultToolkit();
				image = kit.createImage(new MemoryImageSource(nWidth, nHeight, nData, 0, nWidth));
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		} finally {
			if (fs != null) {
				fs.close();
			}
		}
		return image;
	}

	private static void writeFile(Image image, int width, int height, String thumbnailFile) throws Exception {

		if (image == null)
			return;

		BufferedImage tag = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
		tag.getGraphics().drawImage(image, 0, 0, width, height, null);
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(thumbnailFile);
			// 使用标准的ImageIO API替代废弃的JPEGCodec
		ImageIO.write(tag, "jpg", out);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		} finally {
			if (out != null) {
				out.close();
			}
		}

	}

	private static int[] adjustImageSize(int theImgWidth, int theImgHeight, int defWidth, int defHeight) {
		int[] size = { 0, 0 };

		float theImgHeightFloat = Float.parseFloat(String.valueOf(theImgHeight));
		float theImgWidthFloat = Float.parseFloat(String.valueOf(theImgWidth));
		if (theImgWidth < theImgHeight) {
			float scale = theImgHeightFloat / theImgWidthFloat;
			size[0] = Math.round(defHeight / scale);
			size[1] = defHeight;
		} else {
			float scale = theImgWidthFloat / theImgHeightFloat;
			size[0] = defWidth;
			size[1] = Math.round(defWidth / scale);
		}
		return size;
	}

	public static float getScale(BufferedImage sourceImg) {
		if (sourceImg==null){return 0.1F;}
		int max = Math.max(sourceImg.getWidth(), sourceImg.getHeight());
		float scale = 600;
		float scaleF=1.0F;
		if(max>scale){
			scaleF=scale / max;
		}
		return scaleF;
	}



	public static void scaleImgTest(){
		// zoomPicture("E:\\1.jpg","E:\\test.jpg",60, 80);
		//getThumbnail("E:\\9a504fc2d562853582cbf92392ef76c6a7ef638c.jpg", "E:\\test.jpg", 250, 358);

		try {
			String path="D:/data/www/resources/";
			String name="租用价格";
			String type=".png";
			File file = new File(path+name+type);


			BufferedImage src = ImageIO.read(new FileInputStream(file));

			String filePath=path+name;

			float scale = getScale(src);

			ImgUtil.scale(file,

					new File(filePath+"-1"+type),scale);

			try {
				Thumbnails.of(file)
						.scale(scale)
						.toFile(new File(filePath+"-2"+type));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}catch (Exception e){

		}

	}

	public static void main(String[] agrs) throws IOException {

		System.out.println(FileUtils.getSuffixName("src=http___pic1.win4000.com_pic_b_7f_efd9722928.jpg&refer=http___pic1.win4000.jpg"));
	}

}
