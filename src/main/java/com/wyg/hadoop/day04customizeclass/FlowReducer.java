package com.wyg.hadoop.day04customizeclass;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * DESCRIPTION:
 *
 * @author wyg0405@gmail.com
 * @create 2019-02-12 20:29
 **/

public class FlowReducer extends Reducer<Text, FlowEntity, Text, FlowEntity> {
  @Override
  protected void reduce(Text key, Iterable<FlowEntity> values, Context context) throws IOException, InterruptedException {
    int sumUpload = 0;
    int sumDownload = 0;
    for (FlowEntity flowEntity : values) {
      sumUpload = sumUpload + flowEntity.getUpload();
      sumDownload = sumDownload + flowEntity.getDownload();
    }
    FlowEntity flowEntity = new FlowEntity(sumUpload, sumDownload);
    context.write(key, flowEntity);
  }
}
