package com.boyun.proxy;


interface MyInterface{
    void doSomething();
}

class Subject implements MyInterface{

    public Subject() throws InterruptedException {
        Thread.sleep(100);
    }

    @Override
    public void doSomething() {
        System.out.println("do something...");
    }
}

class SubjectProxt implements MyInterface{
    private Subject real;

    public SubjectProxt() {
        //init ...
    }

    @Override
    public void doSomething() {
        if(real == null){
            try {
                real = new Subject();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        real.doSomething();
    }
}

public class ProxyTest {
    public static void main(String[] args) {
        new SubjectProxt().doSomething();
    }
}

