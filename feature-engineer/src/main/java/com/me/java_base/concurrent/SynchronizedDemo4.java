package com.me.java_base.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * 解决死锁方法一：破坏占用且等待条件
 */
public class SynchronizedDemo4 {
    public static void main(String[] args) {
    }
}

class Allocator{
    private List<Object> list = new ArrayList();

    synchronized boolean apply(Object from, Object to){
        if(list.contains(from) || list.contains(to)){
            return false;
        }else{
            list.add(from);
            list.add(to);
        }
        return true;
    }
    synchronized void free(Object from, Object to){
        list.remove(from);
        list.remove(to);
    }

}

class Account2{
    private Allocator allocator;
    private int balance;
    void transfer(Account2 target, int amt){
        while (!allocator.apply(this, target));
    }
}