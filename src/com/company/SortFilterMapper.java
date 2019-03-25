package com.company;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortFilterMapper extends Mapper<LongWritable, Text, DoubleWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings=value.toString().split("\t");
        String[] strings1=strings[1].split("\\|");
//        String[] strings=value.toString().split("\t");
//        String career=strings[0];
//        String[] strings1=strings[1].split("\\|");
//        double longtitude=Double.parseDouble(strings1[1]);
//        double latitude=Double.parseDouble(strings1[2]);
        try {
            double rating= Double.parseDouble(strings1[6]);
            context.write(new DoubleWritable(rating),new Text(strings[1]));
        }catch (NumberFormatException ignored){
            context.write(new DoubleWritable(-1),new Text(strings[1]));
        }
    }
}
