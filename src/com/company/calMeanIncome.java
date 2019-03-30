package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class calMeanIncome {
    public static void main(String[] args) throws FileNotFoundException {
        File file=new File("/home/xiao/文档/大数据分析/数据/D_Standard/part-r-00000");
        Scanner in=new Scanner(file);
        int sum=0;
        int count=0;
        while (in.hasNext()){
            String line=in.nextLine();
            String[] strings=line.split("\\|");
            String s=strings[strings.length-1];
            int val=0;
            if (!s.equals("?")) {
                val = Integer.parseInt(strings[strings.length - 1]);
                count++;
            }
            sum+=val;
        }
        System.out.println(sum/count);
    }
}
