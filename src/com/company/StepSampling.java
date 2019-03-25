package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StepSampling {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"StepSampling");
        job.setMapperClass(StepSamplingMapper.class);
        job.setCombinerClass(StepSamplingCombiner.class);
        job.setReducerClass(StepSamplingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("/home/xiao/文档/大数据分析/数据/large_data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/home/xiao/文档/大数据分析/数据/D_Sample"));
        System.exit(job.waitForCompletion(true)?0:1);
    }

}


