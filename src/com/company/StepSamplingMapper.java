package com.company;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;


public class StepSamplingMapper extends Mapper<Object, Text,Text,Text> {
    private Text user_career=new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String s=value.toString();
        String[] strings=s.split("\\|");
        user_career.set(strings[10]);
        context.write(user_career,new Text(s));
    }
}
