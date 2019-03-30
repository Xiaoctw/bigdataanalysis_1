package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Standard {

    public static void main(String[] args) throws Exception{
        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration,"Standard");
        job.setMapperClass(StandardMapper.class);
        job.setReducerClass(StandardReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("/home/xiao/文档/大数据分析/数据/D_Filtered/part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("/home/xiao/文档/大数据分析/数据/D_Standard"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
 class StandardMapper extends Mapper<LongWritable, Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings=value.toString().split("\t");
        double rating= Double.parseDouble(strings[0]);
        String rating1=Normalizing(rating);
        String[] strings1=strings[1].split("\\|");
        String temperature=changeTemperature(strings1[5]);
        String birthday=StandardData(strings1[8]);
        String review=StandardData(strings1[4]);
        if(rating==-1){
            String res=strings1[0]+"|"+strings1[1]+"|"+strings1[2]+"|"+strings1[3]+"|"+review
                    +"|"+temperature+"|"+"?"+"|"+strings1[7]+"|"+birthday+"|"+strings1[9]+"|"+strings1[10]+"|"+strings1[11];
            context.write(new Text(String.valueOf(rating)),new Text(res));
        }else {
        String res=strings1[0]+"|"+strings1[1]+"|"+strings1[2]+"|"+strings1[3]+"|"+review
                +"|"+temperature+"|"+rating1+"|"+strings1[7]+"|"+birthday+"|"+strings1[9]+"|"+strings1[10]+"|"+strings1[11];
        context.write(new Text(rating1),new Text(res));
        }
    }
    private String StandardData(String data) {
        if(data.charAt(0)>='0'&&data.charAt(0)<='9'){
            if(data.charAt(4)=='-'){
                return data;
            }else {
                String[] strings=data.split("/");
                return strings[0] + "-" + strings[1] + "-" + strings[2];
            }
        }
        String[] strings=data.split(" ");
        String[] strings1=strings[1].split(",");
        String s=strings1[0]+"-"+strings1[1];
        String month=strings[0];
        switch (month) {
            case "January":
                return "1-" + s;
            case "February":
                return "2-" + s;
            case "March":
                return "3-" + s;
            case "April":
                return "4-" + s;
            case "May":
                return "5-" + s;
            case "June":
                return "6-" + s;
            case "July":
                return "7-" + s;
            case "August":
                return "8-" + s;
            case "September":
                return "9-" + s;
            case "October":
                return "10-" + s;
            case "November":
                return "11-" + s;
            default:
                return "12" + s;
        }
    }
    private String Normalizing(Double rate){
        return String.format("%.2f",(rate-59.11)/(95.89-59.11));
    }
    private String changeTemperature(String s){
        int len=s.length();
        double num= Double.parseDouble(s.substring(0,len-2));
        if(s.charAt(len-1)=='℃'){
            return String.format("%.1f", num)+"℃";
        }else {
            double a=(num-32)/1.8;
            return String.format("%.1f",a)+"℃";
        }
    }
}
class StandardReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key,value);
        }
    }
}
