package main.java;

import java.util.Random;

public class Utils {

    private static Random random = new Random(System.currentTimeMillis());


    public static int getRandomInt(int bound){
        return random.nextInt(bound);
    }
}
