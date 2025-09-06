package com.basic.utils.encrypt;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;



public class MD5 {
    public static byte[] encrypt(byte[] inStr) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(inStr);
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            // MD5不可能不支持，
            throw new RuntimeException(e);
        }
    }

    public static byte[] encrypt(String in) {
        return (encrypt(in.getBytes()));
    }

    public static String encryptHex(String in) {
        return HEX.encode(encrypt(in));
    }

    public static String encryptHex(byte[] in) {
        return HEX.encode(encrypt(in));
    }

}
