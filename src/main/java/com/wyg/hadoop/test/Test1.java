package com.wyg.hadoop.test;

/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 14:36
 **/

public class Test1 {
  public static void main(String[] args) {
    //System.out.println("A".hashCode());
    //System.out.println(Integer.MAX_VALUE);
    StringBuffer sb = new StringBuffer();
    String a = "B,C,D,G";
    String[] strings = a.split(",");
    for (String s : strings) {
      sb.append(",").append(s);
    }

    System.out.println(sb.substring(1));
  }

}
