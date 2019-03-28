package com.wyg.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * hbase分页实现
 *
 * @author wyg0405 @gmail.com
 * @create 2019 -03-14 18:52
 */
public class MyPage {
  /**
   * The Conf.
   */
  Configuration conf = null;
  /**
   * The Conn.
   */
  Connection conn = null;
  /**
   * The Table.
   */
  Table table = null;

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws IOException the io exception
   */
  public static void main(String[] args) throws IOException {
    MyPage page = new MyPage();
    page.init();
    ResultScanner scanner = page.getByPage(3, 3);
    //ResultScanner scanner=page.getData("",3);
    for (Result result : scanner) {
      System.out.println(result.toString());
    }
    page.destory();
  }

  /**
   * Init.
   *
   * @throws IOException the io exception
   */
  void init() throws IOException {
    conf = new Configuration();
    //conf.set("fs.defaultFS","hdfs://master");

    //hbase的zookeeper地址
    conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3,hadoop4");

    conn = ConnectionFactory.createConnection(conf);
    table = conn.getTable(TableName.valueOf("stu1"));
  }

  /**
   * Destory.
   *
   * @throws IOException the io exception
   */
  void destory() throws IOException {
    table.close();
    conn.close();
  }

  /**
   * Gets by page.
   *
   * @param page the page
   * @param size the size
   * @return the by page
   * @throws IOException the io exception
   */
  public ResultScanner getByPage(int page, int size) throws IOException {
    ResultScanner scanner = null;

    if (page <= 1) {
      scanner = getData("".getBytes(), size);
    } else {
      ResultScanner results;
      byte[] lastKey = "".getBytes();
      byte[] rowKey = "".getBytes();
      for (int i = 1; i < page; i++) {

        results = getData(rowKey, size);
        for (Result result : results) {
          lastKey = result.getRow();
          System.out.println(Bytes.toString(lastKey));
        }
        rowKey = Bytes.add(lastKey, Bytes.toBytes("0x00"));
      }

      scanner = getData(rowKey, size);
    }

    return scanner;
  }

  /**
   * Gets data.
   *
   * @param rowKey   the row key
   * @param pageSize the page size
   * @return the data
   * @throws IOException the io exception
   */
  public ResultScanner getData(byte[] rowKey, int pageSize) throws IOException {


    Scan scan = new Scan();
    scan.setStartRow(rowKey);
    PageFilter filter = new PageFilter(pageSize);
    scan.setFilter(filter);
    ResultScanner results = table.getScanner(scan);
    return results;
  }

}
