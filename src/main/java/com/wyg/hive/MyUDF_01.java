package com.wyg.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author wyg0405@gmail.com
 * @description:
 * @date 2019-02-26 下午2:20
 **/

public class MyUDF_01 extends UDF {

  public static void main(String[] args) {
    MyUDF_01 udf = new MyUDF_01();

    System.out.println(udf.evaluate("192.0.0.2"));
  }

  /**
   * @param [a, b]
   * @return int
   * @throws
   * @description
   * @author wyg0405@gmail.com
   * @date 19-2-26 下午3:14
   */
  public int evaluate(int a, int b) {
    return a + b;
  }

  /**
   * @param [ip]
   * @return java.lang.String
   * @throws
   * @description 补全ip
   * @author wyg0405@gmail.com
   * @date 19-2-26 下午3:16
   */
  public String evaluate(String ip) {
    String[] spilt = ip.split("\\.");
    StringBuffer sb = new StringBuffer();
    for (String s : spilt) {
      s = "000" + s;
      sb.append(s.substring(s.length() - 3)).append(".");

    }
    return sb.substring(0, sb.length() - 1);
  }
}
