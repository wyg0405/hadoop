package com.wyg.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.List;

/**
 * @author wyg0405@gmail.com
 * @description: 配置文件监听端
 * @date 2019-03-07 下午10:29
 **/

public class ConfigServer {
  public static String CONNECTION = "hadoop1:2181,hadoop2:2181,hadoop3:2181,hadoop4:2181";
  public static String ROOTPATH = "/config";
  public static ZooKeeper zk = null;
  public static List<String> children = null;

  /**
   * @param args
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    zk = new ZooKeeper(CONNECTION, 5000, new Watcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        List<String> newChildren = null;
        try {
          newChildren = zk.getChildren(ROOTPATH, true);
          if (Event.EventType.NodeChildrenChanged.equals("NodeChildrenChanged")) {
            String diffNode = "";
            String operate = "";
            for (String child : children) {
              if (!newChildren.contains(child)) {//节点删除
                diffNode = child;
                operate = "del";
              }
            }
            for (String child : newChildren) {
              if (!children.contains(child)) {//新增节点
                diffNode = child;
                operate = "create";
              }
            }
            System.out.println("操作：" + operate + ",节点：" + diffNode);
            zk.getChildren(ROOTPATH, true);
          }
        } catch (KeeperException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    if (zk.exists(ROOTPATH, null) == null) {
      zk.create(ROOTPATH, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    //子节点增删监听
    children = zk.getChildren(ROOTPATH, true);
    //子节点内容监听
    if (children != null && children.size() > 0) {
      for (String child : children) {
        zk.getData(ROOTPATH + "/" + child, true, null);
      }
    }

    Thread.sleep(Long.MAX_VALUE);
    //zk.close();
  }
}
