package com.me.java_base.concurrent;

/**
 * 死锁问题:两个线程占用了对方需要的资源
 */
public class SynchronizedDemo3 {
    public static void main(String[] args) throws InterruptedException {
        Account1 countA = new Account1();
        Account1 countB = new Account1();
        countA.setBalance(200000);
        countA.setId(1);
        countB.setBalance(200000);
        countB.setId(2);


        Runnable runnableA = new Runnable() {
            @Override
            public void run() {
                try {
                    countA.transfer(countB, 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        Runnable runnableB = new Runnable() {
            @Override
            public void run() {
                try {
                    countB.transfer(countA, 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        Thread thread1 = new Thread(runnableA);
        Thread thread2 = new Thread(runnableB);
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        System.out.println(thread1.getName());
        System.out.println(thread2.getName());

        System.out.println(countA.getBalance());
        System.out.println(countB.getBalance());
    }
}

class Account1 {
    private int balance;
    private int id;

    public void setId(int id) {
        this.id = id;
    }

    public void setBalance(int balance) {
        this.balance = balance;
    }

    public int getBalance() {
        return balance;
    }

    void transfer(Account1 target, int amt) throws InterruptedException {
        Account1 left = this;
        Account1 right = target;
        if(left.id > right.id){
            left = target;
            right = this;
        }
        synchronized (left) {
            Thread.sleep(1000);
            synchronized (right) {
                if (this.balance >= amt) {
                    this.balance -= amt;
                    target.balance += amt;
                }
            }
        }
    }
}
