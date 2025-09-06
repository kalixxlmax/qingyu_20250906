package com.basic.utils.encrypt;

import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.basic.utils.Base64;

@SuppressWarnings({"WeakerAccess", "unused"})
public class DES {
    private static byte[] iv = {1, 2, 3, 4, 5, 6, 7, 8};

    public static byte[] encrypt(byte[] encrypted, byte[] encryptKey) {
        try {
            if (encryptKey.length != 24) {
                encryptKey = Arrays.copyOfRange(encryptKey, 0, 24);
            }
            IvParameterSpec zeroIv = new IvParameterSpec(iv);
            SecretKeySpec key = new SecretKeySpec(encryptKey, "DESede");
            Cipher cipher = Cipher.getInstance("DESede/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, key, zeroIv);
            // 加密
            return cipher.doFinal(encrypted);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] encrypt(String userId, byte[] key) {
        return encrypt(userId.getBytes(), key);
    }

    public static String encryptBase64(byte[] content, byte[] key) {
    	
        return Base64.encode(encrypt(content, key));
    }

    public static String encryptBase64(String content, byte[] key) {
        return Base64.encode(encrypt(content.getBytes(), key));
    }

    public static byte[] decrypt(byte[] content, byte[] decryptKey) {
        try {
            if (decryptKey.length != 24) {
                decryptKey = Arrays.copyOfRange(decryptKey, 0, 24);
            }
            IvParameterSpec zeroIv = new IvParameterSpec(iv);
            SecretKeySpec key = new SecretKeySpec(decryptKey, "DESede");
            Cipher cipher = Cipher.getInstance("DESede/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, key, zeroIv);
            // 解密
            return cipher.doFinal(content);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String decryptString(byte[] content, byte[] key) {
        return new String(decrypt(content, key));
    }

    public static String decryptStringFromBase64(String content, byte[] key) {
        return new String(decrypt(Base64.decode(content), key));
    }
}
