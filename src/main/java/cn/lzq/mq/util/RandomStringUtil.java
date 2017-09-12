package cn.lzq.mq.util;

import java.util.Random;

public class RandomStringUtil {
    private static final String RANDOM_BASE_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int RANDOM_BASE_STRING_LENGHT = RANDOM_BASE_STRING.length();
    private static final Random RANDOM = new Random();

    public static String getRandomString(int length) {
        char[] charArr = new char[length];
        for (int i = 0; i < length; i++) {
            int number = RANDOM.nextInt(RANDOM_BASE_STRING_LENGHT);
            charArr[i] = RANDOM_BASE_STRING.charAt(number);
        }
        return new String(charArr);
    }
}
