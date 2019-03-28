package com.wyg.hadoop.day01hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * DESCRIPTION:给定目录下所有文件单词统计,非hdfs下的方案
 *
 * @author wyg0405@gmail.com
 * @create 2019-01-23 17:25
 **/

public class MyWordCount {

  public static void main(String[] args) throws IOException {

    String path = args[0];
    List<Map<String, Integer>> list = new ArrayList<Map<String, Integer>>();
    list = map(path, list);
    Map<String, Integer> map = reduce(list);
    Set<Map.Entry<String, Integer>> set = map.entrySet();
    for (Map.Entry<String, Integer> entry : set) {
      System.out.println(entry.getKey() + "\t" + entry.getValue());
    }

  }

  public static List<Map<String, Integer>> map(String path, List<Map<String, Integer>> list) throws IOException {
    File file = new File(path);
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files.length > 0) {
        for (File fileDir : files) {
          map(fileDir.getPath(), list);
        }
      }

    } else {

      Map<String, Integer> resMap = new HashMap();
      BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
      String line = "";
      while ((line = bufferedReader.readLine()) != null) {
        String[] split = line.split(" ");
        for (String word : split) {
          if (!resMap.containsKey(word)) {
            resMap.put(word, 1);
          } else {
            resMap.put(word, resMap.get(word) + 1);
          }
        }
      }
      list.add(resMap);
    }

    return list;
  }

  /* public  static Map<String ,Integer> handleFile(String path) throws IOException {
   *//*System.out.println(path);*//*
        Map<String,Integer> resMap=new HashMap();
        BufferedReader bufferedReader=new BufferedReader(new FileReader(path));
        String line="";
        while ((line=bufferedReader.readLine())!=null){
            String[] split=line.split(" ");
            for (String word:split){
                if (!resMap.containsKey(word)){
                    resMap.put(word,1);
                }else{
                    resMap.put(word,resMap.get(word)+1);
                }
            }
        }
        *//*System.out.println(resMap);*//*
        return resMap;
    }*/

  public static Map<String, Integer> reduce(List<Map<String, Integer>> list) {
    Map<String, Integer> resMap = new HashMap<>();
    for (Map<String, Integer> map : list) {
      Set<Map.Entry<String, Integer>> set = map.entrySet();
      for (Map.Entry<String, Integer> entry : set) {
        if (!resMap.containsKey(entry.getKey())) {
          resMap.put(entry.getKey(), entry.getValue());
        } else {
          resMap.put(entry.getKey(), resMap.get(entry.getKey()) + entry.getValue());
        }
      }
    }
    return resMap;
  }
}
