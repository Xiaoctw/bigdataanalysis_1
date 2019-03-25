package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import spire.math.Sort;

public class SortFilter {

    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"FilterSort");
        job.setMapperClass(SortFilterMapper.class);
        job.setReducerClass(SortFilterReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("/home/xiao/文档/大数据分析/数据/D_Sample/part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("/home/xiao/文档/大数据分析/数据/D_SortFilter"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
