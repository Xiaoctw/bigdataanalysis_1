package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.*;

class Item{
    double income;
    double longitude;
    double latitude;
    double rating;

    public Item(double income,  double latitude,double longitude, double rating) {
        this.income = income;
        this.longitude = longitude;
        this.latitude = latitude;
        this.rating = rating;
    }

    public Item(double income, double latitude,double longitude) {
        this.income = income;
        this.longitude = longitude;
        this.latitude = latitude;
    }
}
public class calMissRating {
    private static double maxIncome,minIncome,maxLatitude,minLatitude,maxLongitude,minLongitude;
    private static List<Item> list;
    public static void main(String[] args) throws FileNotFoundException {
        File file=new File("/home/xiao/文档/大数据分析/数据/D_datapreprocess/part-r-00000");
        File outFile=new File("/home/xiao/文档/大数据分析/数据/res.txt");
        PrintStream s=new PrintStream(outFile);
        System.setOut(s);
        Scanner in=new Scanner(file);
        maxIncome=Integer.MAX_VALUE;
        maxLatitude=Integer.MAX_VALUE;
        maxLongitude=Integer.MAX_VALUE;
        minIncome=Integer.MIN_VALUE;
        minLatitude=Integer.MIN_VALUE;
        minLongitude=Integer.MIN_VALUE;
        while (in.hasNext()){
            String line=in.nextLine();
            String[] s1=line.split("\t");
            String[] strings=s1[1].split("\\|");
            double income= Double.parseDouble(strings[11]);
            double longitude= Double.parseDouble(strings[1]);
            double latitude= Double.parseDouble(strings[2]);
            if (income>maxIncome){
                maxIncome=income;
            }
            if(income<minIncome){
                minIncome=income;
            }
            if(longitude>maxLongitude){
                maxLongitude=longitude;
            }
            if(longitude<minLongitude){
                minLongitude=longitude;
            }
            if(latitude>maxLatitude){
                maxLongitude=latitude;
            }
            if(latitude<minLatitude){
                minLatitude=latitude;
            }
        }
        in.close();
        in=new Scanner(file);
        Map<Integer,String> map=new HashMap<>();
        Map<Integer,Item> map1=new HashMap<>();
        list=new ArrayList<>();
        int i=0;
        while (in.hasNext()){
            String line1=in.nextLine();
            String line=line1.split("\t")[1];
            String[] strings=line.split("\\|");
            double income= Double.parseDouble(strings[11]);
            double longitude= Double.parseDouble(strings[1]);
            double latitude= Double.parseDouble(strings[2]);
            if(strings[6].equals("?")){
                map1.put(i,new Item(standard(0,income),standard(1,latitude),standard(2,longitude)));
                map.put(i,line);
                i++;
            }else {
                double rating= Double.parseDouble(strings[6]);
                list.add(new Item(standard(0,income),standard(1,latitude),standard(2,longitude),rating));
                System.out.println(line);
            }
        }
        for (int j = 0; j <i ; j++) {
            map1.get(j).rating=findRat(map1.get(j));
        }
        for (int j = 0; j <i ; j++) {
            String[] strings1=map.get(j).split("\\|");
            String res=strings1[0]+"|"+strings1[1]+"|"+strings1[2]+"|"+strings1[3]+"|"+strings1[4]
                    +"|"+strings1[5]+"|"+String.format("%.2f",map1.get(j).rating)+"|"+strings1[7]+"|"+strings1[8]+"|"+strings1[9]+"|"+strings1[10]+"|"+strings1[11];
            System.out.println(res);
        }
    }
    private static double standard(int key,double val){
        if(key==0){
            return val-minIncome/(maxIncome-minIncome);
        }else if(key==1){
            return val-minLatitude/(maxLatitude-minLatitude);
        }
        return val-minLongitude/(maxLongitude-minLongitude);
    }
    private static double findRat(Item item){
        list.sort((o1, o2) -> {
            double dis1=Math.pow((o1.income-item.income),2)+Math.pow((o1.latitude-item.latitude),2)+Math.pow(o1.longitude-item.longitude,2);
            double dis2=Math.pow((o2.income-item.income),2)+Math.pow((o2.latitude-item.latitude),2)+Math.pow(o2.longitude-item.longitude,2);
          //  return (int) (dis1*100-dis2*100);
            return Double.compare(dis1, dis2);
        });
        return list.get(0).rating;
    }
}
