package com.me.java_base.concurrent;

public class Excecise01 {
    long value = 0L;

    long getValue() {
        return value;
    }

    /**
     * 这里this可以替换成new Object() 给不同的对象加锁，观察结果是否一致
     */
    void addOne() {
        synchronized (this) {
            value += 1;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Excecise01 excecise01 = new Excecise01();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < 10000; i++) {
                    excecise01.addOne();
                }
            }
        };
        for (int i = 0; i < 100; i++) {
            Thread thread = new Thread(runnable);
            thread.start();
            thread.join();
        }

        System.out.println(excecise01.getValue());
    }
}
