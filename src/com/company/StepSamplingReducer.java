package com.company;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StepSamplingReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list=new ArrayList<>();
        for (Text value:values) {
            list.add(value.toString());
        }
        int num=list.size();
        int sampleNum;
        if(num<100){
            sampleNum=num/20;
        }else {
            sampleNum=num/60;
        }
        int[] indexes=new int[sampleNum];
        for (int j = 0; j < num; j++) {
            if(j<sampleNum){
                indexes[j]=j;
            }else {
                int index= (int) (Math.random()*j);
                if(index<sampleNum){
                    indexes[index]=j;
                }
            }
        }
        for (int i = 0; i < sampleNum; i++) {
            context.write(key,new Text(list.get(indexes[i])));
        }
    }
}
class StepSamplingCombiner extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            if(Math.random()>0.5){
                context.write(key,value);
            }
        }
    }
}