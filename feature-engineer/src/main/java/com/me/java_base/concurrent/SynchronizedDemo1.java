package com.me.java_base.concurrent;

public class SynchronizedDemo1 {

    public static void main(String[] args) throws InterruptedException {
        SynchronizedDemo1 demo1 = new SynchronizedDemo1();
        MyRunnable myRunnable = demo1.new MyRunnable();
        Thread thread1 = new Thread(myRunnable);
        Thread thread2 = new Thread(myRunnable);
        thread1.start();
        thread2.start();
        thread2.join();
        thread1.join();
        System.out.println(myRunnable.getVar());
    }

    class MyRunnable implements Runnable{
        Test test = new Test();
        public int getVar(){
            return test.var;
        }
        @Override
        public void run() {
            for (int i = 0; i < 500000; i++) {
                test.add();
            }
            System.out.println("debug");
        }
    }
}
class Test{
    volatile int var = 0;
     public void add(){
        var += 1;
    }
}
