package com.boyun.proxy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ReflectTest {
    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        System.out.println(Integer.class);
        System.out.println(String.class);
        System.out.println(Class.forName("java.lang.Integer").getName());
        Constructor<Integer> constructor = Integer.class.getConstructor(int.class);
        System.out.println(constructor.newInstance(10));

        System.out.println(Boolean.TYPE);
    }
}

class A{
    public void hi(){
        System.out.println("hi...");
    }
}
