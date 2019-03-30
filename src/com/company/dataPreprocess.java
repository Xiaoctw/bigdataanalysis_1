package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class dataPreprocess {
    private static int mean_income=2860;
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"dataPreprocess");
        job.setMapperClass(calUserIncomeMapper.class);
        job.setReducerClass(calUserIncomeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("/home/xiao/文档/大数据分析/数据/D_Standard/part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("/home/xiao/文档/大数据分析/数据/D_datapreprocess"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class calUserIncomeMapper extends Mapper<LongWritable, Text, IntWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings=value.toString().split("\t");
//            String rating=strings[0];
            String[] strings1=strings[1].split("\\|");
            String nationality=strings1[9];
            String career=strings1[10];
            int val=nationality.hashCode()+career.hashCode();
            context.write(new IntWritable(val),new Text(strings[1]));
        }
    }
    public static class calUserIncomeReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list1=new ArrayList<>();
            List<String> list2=new ArrayList<>();
            for (Text value : values) {
                String s=value.toString();
                if(s.split("\\|")[11].equals("?")){
                    list1.add(s);
                }else {
                    list2.add(s);
                }
            }
            if(list2.size()==0){
                for (String s : list1) {
                    StringBuilder sb=new StringBuilder();
                    String[] strings=s.split("\\|");
                    for (int i = 0; i <=10; i++) {
                        sb.append(strings[i]).append("|");
                    }
                    sb.append(mean_income);
                    context.write(key,new Text(sb.toString()));
                }
            }else {
                int sum=0;
                int count=0;
                for (String s : list2) {
                    String[] strings=s.split("\\|");
                    sum+=Integer.parseInt(strings[11]);
                    count++;
                    context.write(key,new Text(s));
                }
                int mean=sum/count;
                for (String s : list1) {
                    StringBuilder sb=new StringBuilder();
                    String[] strings=s.split("\\|");
                    for (int i = 0; i <=10; i++) {
                        sb.append(strings[i]).append("|");
                    }
                    sb.append(mean);
                    context.write(key,new Text(sb.toString()));
                }
            }
        }
    }
}
