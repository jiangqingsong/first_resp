package com.me.java_base.concurrent;

/**
 * AccountA转账给AccountB
 *
 * 同一个锁锁多个资源
 */
public class SynchronizedDemo2 {
    public static void main(String[] args) throws InterruptedException {
        Account1 countA = new Account1();
        Account1 countB = new Account1();
        countA.setBalance(200000);
        countB.setBalance(200000);


        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 10000; i++) {
                        countA.transfer(countB, 10);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        Thread thread1 = new Thread(runnable);
        Thread thread2 = new Thread(runnable);
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        System.out.println(countA.getBalance());
        System.out.println(countB.getBalance());
    }
}


class Account {
    private int balance;

    public void setBalance(int balance) {
        this.balance = balance;
    }

    public int getBalance() {
        return balance;
    }

    void transfer(Account target, int amt) throws InterruptedException {
        //用 Account.class 作为共享的锁
        synchronized (Account1.class) {
            if (target.balance >= amt) {
                this.balance -= amt;
                target.balance += amt;
            }
        }
    }
}
