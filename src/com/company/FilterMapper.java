package com.company;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings=value.toString().split("\t");
        String rating1=strings[0];
        String[] strings1=strings[1].split("\\|");
        double rating=Double.parseDouble(strings[0]);
        double longtitude=Double.parseDouble(strings1[1]);
        double latitude=Double.parseDouble(strings1[2]);
        if(rating==-1){
            context.write(new Text(rating1),new Text(strings[1]));
        }
        if (rating>=59.11&&rating<=95.89&&longtitude>=8.1461259&&longtitude<=11.1993265&&latitude>=56.5824856&&latitude<=57.750511){
            context.write(new Text(rating1),new Text(strings[1]));
        }
    }
}
