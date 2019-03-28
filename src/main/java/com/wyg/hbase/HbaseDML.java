package com.wyg.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wyg0405@gmail.com
 * @create 2019-03-13 18:03
 **/

public class HbaseDML {
  public static Configuration conf = null;
  public static Connection connection = null;
  public static Admin admin = null;
  private Table table = null;

  /**
   * 初始化，创建连接
   *
   * @throws IOException
   */
  @Before
  public void init() throws IOException {
    //配置文件
    conf = HBaseConfiguration.create();
    //zookeeper地址
    conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3,hadoop4");
    //创建连接
    connection = ConnectionFactory.createConnection(conf);

    System.out.println(connection.toString());

    admin = connection.getAdmin();

    table = connection.getTable(TableName.valueOf("test1"));
  }

  /**
   * 关闭连接
   *
   * @throws IOException
   */
  @After
  public void destroy() throws IOException {

    admin.close();
    table.close();
    connection.close();
  }

  /**
   * 单个插入
   *
   * @throws IOException
   */
  @Test
  public void putOne() throws IOException {

    Put put = new Put("rk_001".getBytes());
    put.addColumn("info".getBytes(), "name".getBytes(), "lucy".getBytes());
    put.addColumn("score".getBytes(), "math".getBytes(), Bytes.toBytes(98));
    table.put(put);
  }

  /**
   * 插入多条
   *
   * @throws IOException
   */
  @Test
  public void outMany() throws IOException {
    List<Put> puts = new ArrayList<>();
    Put p1 = new Put(Bytes.toBytes("rk_002"));
    p1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("lily"));
    p1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(18));
    p1.addColumn(Bytes.toBytes("score"), Bytes.toBytes("math"), Bytes.toBytes(96));
    puts.add(p1);

    Put p2 = new Put(Bytes.toBytes("rk_003"));
    p2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
    p2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(19));
    p2.addColumn(Bytes.toBytes("score"), Bytes.toBytes("english"), Bytes.toBytes(80));
    puts.add(p2);

    Put p3 = new Put(Bytes.toBytes("rk_004"));
    p3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("jack"));
    p3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(18));
    puts.add(p3);

    table.put(puts);
  }

  /**
   * 删除
   *
   * @throws IOException
   */
  @Test
  public void delRecord() throws IOException {
    //删除行键为rk_002记录
    Delete delete = new Delete(Bytes.toBytes("rk_002"));
    //table.delete(delete);

    //删除行键rk_003列簇为info的记录
    Delete del2 = new Delete(Bytes.toBytes("rk_003"));
    del2.addFamily(Bytes.toBytes("info"));
    //table.delete(del2);

    //删除行键rk_003列簇为info,列为age的记录
    Delete del3 = new Delete(Bytes.toBytes("rk_004"));
    del3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
    table.delete(del3);
  }

  /**
   * get查询
   *
   * @throws IOException
   */
  @Test
  public void getData() throws IOException {
    Get get = new Get(Bytes.toBytes("rk_002"));
    //get.addFamily(Bytes.toBytes("info")); //指定获取某个列族
    //get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));//指定获取某个列族中的某个列
    Result result = table.get(get);
    System.out.println(result);
    printResult(result);
  }


  /**
   * scan
   *
   * @throws IOException
   */
  @Test
  public void scan() throws IOException {
    Scan scan = new Scan();
    //行健是以字典序排序，可以使用scan.setStartRow()，scan.setStopRow()设置行健的字典序
    //scan.addFamily(Bytes.toBytes("info"));// 只查询列族info
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));// 只查询列name
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));//只查询列age
    ResultScanner results = table.getScanner(scan);
    System.out.println(results);
    for (Result result : results) {
      printResult(result);
    }
  }

  /**
   * CompareOp 是一个枚举，有如下几个值
   * LESS                 小于
   * LESS_OR_EQUAL        小于或等于
   * EQUAL                等于
   * NOT_EQUAL            不等于
   * GREATER_OR_EQUAL     大于或等于
   * GREATER              大于
   * NO_OP                无操作
   */
  @Test
  public void singleColumnValueFilter() throws IOException {
    SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
            CompareFilter.CompareOp.EQUAL, Bytes.toBytes("lily"));//查询列name为lily的记录
    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner results = table.getScanner(scan);
    for (Result result : results) {
      printResult(result);
    }
  }

  /**
   * 列名前缀过滤器（过滤指定前缀的列名） ColumnPrefixFilter
   *
   * @throws IOException
   */
  @Test
  public void columnPrefixFilter() throws IOException {
    // 查询列以name开头列的数据
    ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("na"));
    ResultScanner results = table.getScanner(new Scan().setFilter(filter));
    for (Result result : results) {
      printResult(result);
    }
  }

  /**
   * 全表扫描：多个列名前缀过滤器（过滤多个指定前缀的列名） MultipleColumnPrefixFilter
   *
   * @throws IOException
   */
  @Test
  public void multipleColumnPrefixFilter() throws IOException {
    // 查询列以na或a开头列的数据
    byte[][] bytes = new byte[][]{Bytes.toBytes("na"), Bytes.toBytes("a")};
    MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(bytes);
    ResultScanner results = table.getScanner(new Scan().setFilter(filter));
    for (Result result : results) {
      printResult(result);
    }
  }

  @Test
  public void rowFilter() throws IOException {
    // 匹配rowkey以rk_00开头的数据
    RowFilter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^rk_003"));
    // 匹配rowkey以2结尾的数据
    RowFilter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("2$"));
    Scan scan = new Scan();
    scan.setFilter(filter1);
    ResultScanner results1 = table.getScanner(scan);
    for (Result result : results1) {
      printResult(result);
    }
    scan.setFilter(filter2);
    ResultScanner results2 = table.getScanner(scan);
    for (Result result : results2) {
      printResult(result);
    }
  }

  @Test
  public void mutiFilterTest() throws IOException {
    /**
     * Operator 为枚举类型，有两个值 MUST_PASS_ALL 表示 and，MUST_PASS_ONE 表示 or
     */
    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    //查询年龄大于18
    SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("info"),
            Bytes.toBytes("age"), CompareFilter.CompareOp.GREATER, Bytes.toBytes(18));
    //rowkey以1结尾
    RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^rk_00"));
    filters.addFilter(singleColumnValueFilter);
    filters.addFilter(rowFilter);
    ResultScanner results = table.getScanner(new Scan().setFilter(filters));
    for (Result result : results) {
      printResult(result);
    }

  }


  /**
   * 结果打印
   *
   * @param result
   */
  public void printResult(Result result) {
    byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
    byte[] age = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
    byte[] gender = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("gender"));
    byte[] math = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("math"));
    byte[] english = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("english"));
    if (name != null) {
      System.out.println("name:" + Bytes.toString(name));
    }
    if (age != null) {
      System.out.println("age:" + Bytes.toInt(age));
    }
    if (gender != null) {
      System.out.println("gender:" + Bytes.toString(gender));
    }
    if (math != null) {
      System.out.println("math:" + Bytes.toInt(math));
    }
    if (english != null) {
      System.out.println("english:" + Bytes.toInt(english));
    }
    System.out.println("=====================================");
  }

}
