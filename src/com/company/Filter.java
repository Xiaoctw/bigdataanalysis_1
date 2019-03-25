package com.company;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Filter {

    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"Filter");
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("/home/xiao/文档/大数据分析/数据/D_SortFilter/part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("/home/xiao/文档/大数据分析/数据/D_Filtered"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
