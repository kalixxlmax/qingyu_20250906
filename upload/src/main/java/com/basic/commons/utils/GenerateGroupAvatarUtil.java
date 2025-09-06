package com.basic.commons.utils;

import sun.misc.BASE64Encoder;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.RoundRectangle2D;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * GenerateGroupAvatarUtil <br>
 *
 * @author:  <br>
 * @date: 2021/1/9 0009  <br>
 */
public class GenerateGroupAvatarUtil {

    /**
     * 图片格式：JPG
     */
    public static final String PICTRUE_FORMATE_JPG = "JPG";

    /**
     * 生成组合头像 (返回Base64字符串)
     *
     * @param paths 用户图像
     * @throws IOException
     */
    public static String getCombinationOfHead(List<String> paths,String outpath)
            throws IOException, URISyntaxException {


        List<BufferedImage> bufferedImages = new ArrayList<BufferedImage>();
        // 压缩图片所有的图片生成尺寸同意的 为 50x50

        int imageSize = 33;
        if (paths.size() <= 4) {
            imageSize = 50;
        }

        for (int i = 0; i < paths.size(); i++) {
            bufferedImages.add(resize2(paths.get(i), imageSize, imageSize, true));
        }

        int width = 300; // 这是画板的宽高

        int height = 300; // 这是画板的高度

        // BufferedImage.TYPE_INT_RGB可以自己定义可查看API

        BufferedImage outImage = new BufferedImage(width, height,
                BufferedImage.TYPE_INT_RGB);

        // 生成画布
        Graphics g = outImage.getGraphics();

        Graphics2D g2d = (Graphics2D) g;

        // 设置背景色
        g2d.setBackground(new Color(231, 231, 231));
        //g2d.setBackground(new Color(231, 0, 4));

        // 通过使用当前绘图表面的背景色进行填充来清除指定的矩形。
        g2d.clearRect(0, 0, width, height);

        // 开始拼凑 根据图片的数量判断该生成那种样式的组合头像目前为4中
        int j = 1;
        int k = 1;
        for (int i = 1; i <= bufferedImages.size(); i++) {
            if (bufferedImages.size() == 9) {
                if (i <= 3) {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * i + 3 * i - 33, 4, null);
                } else if (i <= 6) {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * j + 3 * j - 33, 41, null);
                    j++;
                } else {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * k + 3 * k - 33, 77, null);
                    k++;
                }
            } else if (bufferedImages.size() == 8) {
                if (i <= 2) {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * i + 4 * i - 18, 4, null);
                } else if (i <= 5) {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * j + 3 * j - 33, 41, null);
                    j++;
                } else {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * k + 3 * k - 33, 77, null);
                    k++;
                }
            } else if (bufferedImages.size() == 7) {
                if (i <= 1) {
                    g2d.drawImage(bufferedImages.get(i - 1), 39, 4, null);
                } else if (i <= 4) {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * j + 3 * j - 33, 41, null);
                    j++;
                } else {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * k + 3 * k - 33, 77, null);
                    k++;
                }
            } else if (bufferedImages.size() == 6) {
                if (i <= 3) {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * i + 3 * i - 33, 15, null);
                } else {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * j + 3 * j - 33, 58, null);
                    j++;
                }
            } else if (bufferedImages.size() == 5) {
                if (i <= 2) {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * i + 4 * i - 18, 15, null);
                } else {
                    g2d.drawImage(bufferedImages.get(i - 1), 33 * j + 3 * j - 33, 58, null);
                    j++;
                }
            } else if (bufferedImages.size() == 4) {
                if (i <= 2) {
                    g2d.drawImage(bufferedImages.get(i - 1), 50 * i + 4 * i - 50, 4, null);
                } else {
                    g2d.drawImage(bufferedImages.get(i - 1), 50 * j + 4 * j - 50, 58, null);
                    j++;
                }
            } else if (bufferedImages.size() == 3) {
                if (i <= 1) {
                    g2d.drawImage(bufferedImages.get(i - 1), 31, 4, null);
                } else {
                    g2d.drawImage(bufferedImages.get(i - 1), 50 * j + 4 * j - 50, 58, null);
                    j++;
                }

            } else if (bufferedImages.size() == 2) {

                g2d.drawImage(bufferedImages.get(i - 1), 50 * i + 4 * i - 50,
                        31, null);

            } else if (bufferedImages.size() == 1) {

                g2d.drawImage(bufferedImages.get(i - 1), 31, 31, null);

            }

            // 需要改变颜色的话在这里绘上颜色。可能会用到AlphaComposite类
        }


        String format = PICTRUE_FORMATE_JPG;

        // TODO 也可以保存到本地路径
        // String outPath = "E:\\b.jpg";
        ImageIO.write(outImage, format, new File(outpath));


        return null;
        // 生成Base64字符串
        /*String imageString = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ImageIO.write(outImage, format, bos);
            byte[] imageBytes = bos.toByteArray();
            BASE64Encoder encoder = new BASE64Encoder();
            imageString = encoder.encode(imageBytes);
            bos.close();
        } catch (IOException process) {
            process.printStackTrace();
        }

        return "data:image/jpeg;base64," + imageString;*/
    }

    /**
     * 图片缩放
     *
     * @param filePath 图片路径
     * @param height   高度
     * @param width    宽度
     * @param bb       比例不对时是否需要补白
     */
    private static BufferedImage resize2(String filePath, int height, int width,
                                  boolean bb) throws URISyntaxException {
        try {
            double ratio = 0; // 缩放比例

            DataInputStream dis = new DataInputStream(new File(filePath).toURL().openStream());

            //File f = new File(dis);
            BufferedImage bi = ImageIO.read(dis);
            Image itemp = bi.getScaledInstance(width, height,
                    Image.SCALE_SMOOTH);
            // 计算比例
            if ((bi.getHeight() > height) || (bi.getWidth() > width)) {
                if (bi.getHeight() > bi.getWidth()) {
                    ratio = (new Integer(height)).doubleValue()
                            / bi.getHeight();
                } else {
                    ratio = (new Integer(width)).doubleValue() / bi.getWidth();
                }
                AffineTransformOp op = new AffineTransformOp(
                        AffineTransform.getScaleInstance(ratio, ratio), null);
                itemp = op.filter(bi, null);
            }
            if (bb) {
                // copyimg(filePath, "D:\\img");
                BufferedImage image = new BufferedImage(width, height,
                        BufferedImage.TYPE_INT_RGB);
                Graphics2D g = image.createGraphics();
                g.setColor(Color.white);
                g.fillRect(0, 0, width, height);
                if (width == itemp.getWidth(null)) {
                    g.drawImage(itemp, 0, (int) ((height - itemp.getHeight(null)) / 1.5),
                            itemp.getWidth(null), itemp.getHeight(null),
                            Color.white, null);
                } else {
                    g.drawImage(itemp, (int) ((height - itemp.getHeight(null)) / 1.5), 0,
                            itemp.getWidth(null), itemp.getHeight(null),
                            Color.white, null);
                }
                g.dispose();

                itemp=setClip(image,50);
            }
            return (BufferedImage) itemp;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 图片设置圆角
     * @param srcImage
     * @param radius
     * @param border
     * @param padding
     * @return
     * @throws IOException
     */
    public static BufferedImage setRadius(BufferedImage srcImage, int radius, int border, int padding) throws IOException{
        int width = srcImage.getWidth();
        int height = srcImage.getHeight();
        int canvasWidth = width + padding * 2;
        int canvasHeight = height + padding * 2;

        BufferedImage image = new BufferedImage(canvasWidth, canvasHeight, BufferedImage.TYPE_INT_ARGB);
        Graphics2D gs = image.createGraphics();
        gs.setComposite(AlphaComposite.Src);
        gs.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        gs.setColor(Color.WHITE);
        gs.fill(new RoundRectangle2D.Float(0, 0, canvasWidth, canvasHeight, radius, radius));
        gs.setComposite(AlphaComposite.SrcAtop);
        gs.drawImage(setClip(srcImage, radius), padding, padding, null);
        if(border !=0){
            gs.setColor(Color.GRAY);
            gs.setStroke(new BasicStroke(border));
            gs.drawRoundRect(padding, padding, canvasWidth - 2 * padding, canvasHeight - 2 * padding, radius, radius);
        }
        gs.dispose();
        return image;
    }

    /**
     * 图片设置圆角
     * @param srcImage
     * @return
     * @throws IOException
     */
    public static BufferedImage setRadius(BufferedImage srcImage) throws IOException{
        int radius = (srcImage.getWidth() + srcImage.getHeight()) / 6;
        return setRadius(srcImage, radius, 2, 5);
    }

    /**
     * 图片切圆角
     * @param srcImage
     * @param radius
     * @return
     */
    public static BufferedImage setClip(BufferedImage srcImage, int radius){
        int width = srcImage.getWidth();
        int height = srcImage.getHeight();
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        Graphics2D gs = image.createGraphics();

        gs.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        gs.setClip(new RoundRectangle2D.Double(0, 0, width, height, radius, radius));
        gs.drawImage(srcImage, 0, 0, null);
        gs.dispose();
        return image;
    }


    public static void main(String[] args) {

        String pathStart="D:\\data\\www\\resources\\avatar\\o\\2\\";
        List<String> list=new ArrayList<>();
        list.add(pathStart+"100002.jpg");
        list.add(pathStart+"10000002.jpg");
        list.add(pathStart+"10010002.jpg");
        list.add(pathStart+"10020002.jpg");
        list.add(pathStart+"10040002.jpg");


        try {
            GenerateGroupAvatarUtil.getCombinationOfHead(list,pathStart+"666.jpg");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
