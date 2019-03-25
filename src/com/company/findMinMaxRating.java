package com.company;



import org.json4s.FileInput;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class findMinMaxRating {
    public static void main(String[] args) throws FileNotFoundException {
        File file=new File("/home/xiao/文档/大数据分析/数据/D_SortFilter/part-r-00000");
        Scanner in=new Scanner(new FileInputStream(file));
        int numLine=0;
        while (in.hasNext()){
            numLine++;
            in.nextLine();
        }
        in=new Scanner(new FileInputStream(file));
        int count1=0;
        while (in.hasNext()){
            String line=in.nextLine();
            double val= Double.parseDouble(line.split("\t")[0]);
            if(val>-1){
                count1++;
            }
        }
        double down=0,up=0;
        in=new Scanner(new FileInputStream(file));
        int count2=0;
        while (in.hasNext()){
            String line=in.nextLine();
            double val= Double.parseDouble(line.split("\t")[0]);
            if(val>-1){
                count2++;
                if(count2==count1/100){
                    down=val;
                }else if(count2==count1-count1/100){
                    up=val;
                }
            }
        }
        System.out.println(down);
        System.out.println(up);
    }
}
