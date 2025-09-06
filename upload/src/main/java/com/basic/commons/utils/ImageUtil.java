package com.basic.commons.utils;

import com.basic.commons.JoinLayout;

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
 * ImageUtil <br>
 *
 * @author:  <br>
 * @date: 2021/1/9 0009  <br>
 */
public class ImageUtil {
    /**
     * 图片格式：JPG
     */
    public static final String PICTRUE_FORMATE_JPG = "PNG";

    /**
     * 生成组合头像 (返回Base64字符串)
     *
     * @param paths 用户图像
     * @throws IOException
     */
    public static InputStream  getCombinationOfHead(List<File> paths,String outpath)
            throws IOException, URISyntaxException {


        List<BufferedImage> bufferedImages = new ArrayList<BufferedImage>();
        // 压缩图片所有的图片生成尺寸同意的 为 50x50

        int width = 330; // 这是画板的宽高

        int  imageSize = 100;
        switch (paths.size()){
            case 1:
                imageSize=260;
                break;
            case 2:
                imageSize=160;
                break;
            case 3:
                imageSize=120;
                break;
            case 4:
                imageSize=120;
                break;
            default:
                imageSize=100;
                break;
        }
        for (int i = 0; i < paths.size(); i++) {
            bufferedImages.add(resize2(paths.get(i), imageSize, imageSize, true));
        }




        // BufferedImage.TYPE_INT_RGB可以自己定义可查看API

        BufferedImage outImage = new BufferedImage(width, width,
                BufferedImage.TYPE_INT_RGB);

        // 生成画布
        Graphics g = outImage.getGraphics();

        Graphics2D g2d = (Graphics2D) g;

        // 设置背景色
        g2d.setBackground(new Color(231, 231, 231));
        //g2d.setBackground(new Color(231, 0, 4));

        // 通过使用当前绘图表面的背景色进行填充来清除指定的矩形。
        g2d.clearRect(0, 0, width, width);

        // 开始拼凑 根据图片的数量判断该生成那种样式的组合头像目前为4中
        int j = 1;
        Image image;
        float[] offset=null;
        int count=bufferedImages.size();
        float[] size = JoinLayout.size(count);
        for (int i = 1; i <= bufferedImages.size(); i++) {
             image = bufferedImages.get(i - 1);
            offset = JoinLayout.offset(count, i - 1, 300F, size);

            g2d.drawImage(image, (int)offset[0],  (int)offset[1], null);

            // 需要改变颜色的话在这里绘上颜色。可能会用到AlphaComposite类
        }


        String format = PICTRUE_FORMATE_JPG;

        // TODO 也可以保存到本地路径
        // String outPath = "E:\\b.jpg";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ImageIO.write(outImage, format, outputStream);

        //ImageIO.write(outImage, format, new File(outpath));

        InputStream input = new ByteArrayInputStream(outputStream.toByteArray());
        return input;

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
                    g.drawImage(itemp, 0, (int) ((height - itemp.getHeight(null)) / 2),
                            itemp.getWidth(null), itemp.getHeight(null),
                            Color.white, null);
                } else {
                    g.drawImage(itemp, (int) ((height - itemp.getHeight(null)) / 2), 0,
                            itemp.getWidth(null), itemp.getHeight(null),
                            Color.white, null);
                }
                g.dispose();

                itemp=setClip(image,100);
            }
            return (BufferedImage) itemp;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    private static BufferedImage resize2(File file, int height, int width,
                                         boolean bb) throws URISyntaxException {
        try {
            double ratio = 0; // 缩放比例

            DataInputStream dis = new DataInputStream(file.toURL().openStream());

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
                    g.drawImage(itemp, 0, (int) ((height - itemp.getHeight(null)) / 2),
                            itemp.getWidth(null), itemp.getHeight(null),
                            Color.white, null);
                } else {
                    g.drawImage(itemp, (int) ((height - itemp.getHeight(null)) / 2), 0,
                            itemp.getWidth(null), itemp.getHeight(null),
                            Color.white, null);
                }
                g.dispose();

                itemp=setClip(image,width);
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

    /**
     * 将BufferedImage转换为InputStream
     * @param image
     * @return
     */
    public InputStream bufferedImageToInputStream(BufferedImage image){
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            ImageIO.write(image, "png", os);
            InputStream input = new ByteArrayInputStream(os.toByteArray());
            return input;
        } catch (IOException e) {

        }
        return null;
    }


    public static void main(String[] args) {

        String pathStart="D:\\data\\www\\resources\\avatar\\o\\2\\";
        List<File> list=new ArrayList<>();
        list.add(new File(pathStart+"100002.jpg"));
        //list.add(new File(pathStart+"10000002.jpg"));
        //list.add(new File(pathStart+"10010002.jpg"));
        //list.add(new File(pathStart+"10020002.jpg"));
        //list.add(new File(pathStart+"10040002.jpg"));


        try {
            InputStream inputStream =ImageUtil.getCombinationOfHead(list,pathStart+"666.png");

            //ImageIO.write(outImage, format, new File(outpath));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
