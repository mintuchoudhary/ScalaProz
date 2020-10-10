package com.m2.example;

public class HelloJava {

    public static void main(String[] args) {
        System.out.println("Hello Java");

        try {
            int a = 5/0;
        }
        catch(Exception e) {
            System.out.println("bbb Java");
        }


    }


}
