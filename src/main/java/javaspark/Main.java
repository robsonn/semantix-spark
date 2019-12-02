package javaspark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
     public static void main(String[] args) {                              

          // configure spark
          SparkConf sparkConf = new SparkConf().setAppName("Read log to RDD")
          .setMaster("local[2]").set("spark.executor.memory","2g");

          // start a spark context
          JavaSparkContext sc = new JavaSparkContext(sparkConf);    
          sc.setLogLevel("ERROR");                
          
          try{
               // regex to filter date
               Pattern dateRegex = Pattern.compile("\\d{2}.\\w{3}.\\d{4}");

               // regex to check non digit in string
               Pattern nonNumberRegex = Pattern.compile("\\D+");

               // path to input text file
               String path1 = "access_log_Jul95";
               String path2 = "access_log_Aug95";

               // read text file to RDD
               JavaRDD<String> lines = sc.textFile(path1)
               .union(sc.textFile(path2))
               .cache();

               // read bytes column
               JavaRDD<Long> bytes = lines          
               .map(line -> {
                    Long bt = 0L;
                    if(line.split(" ").length > 9) {
                         Matcher matcher = nonNumberRegex.matcher(line.split(" ")[9]);
                         if(!matcher.find() && !line.split(" ")[9].isEmpty()) {
                              bt = Long.valueOf(line.split(" ")[9]);
                         }
                    }
                    return bt;
               });          
               
               // read host column
               JavaRDD<String> host = lines.map(line -> line.split(" ")[0]);  

               // read lines with error 404
               JavaPairRDD<String, Integer> lines404 = lines
               .filter(line -> {
                    Boolean exist404 = false;
                    if(line.split(" ").length > 8) {                                        
                         exist404 = line.split(" ")[8].equals("404");
                    }
                    return exist404;
               })
               .mapToPair(line -> new Tuple2(line, 1))
               .cache();

               // read hosts with error 404
               JavaPairRDD<String, Integer> hosts404 = lines404
               .map(line -> line._1.split(" ")[0])
               .mapToPair(line -> new Tuple2(line, 1));

               // read date with error 404
               JavaPairRDD<String, Integer> date404 = lines404
               .map(line -> {
                    String dateString = "";
                    Matcher matcher = dateRegex.matcher(line._1.split(" ")[3]);
                    if(matcher.find())
                         dateString = matcher.group();

                    return dateString;    
               })
               .mapToPair(line -> new Tuple2(line, 1)); 
               
               //1) Número de hosts únicos.
               System.out.println("hosts unicos: " + host.distinct().count());

               //2) O total de erros 404.
               System.out.println("total de erros 404:  " + lines404.count());                    

               //3) Os 5 URLs que mais causaram erro 404.
               System.out.println("as  5 urls que mais causaram erro 404:\r\n" + String.join("\r\n", hosts404                    
               .reduceByKey((a,b) -> a + b)          
               .map(item -> item._2 + "_" + item._1)
               .sortBy((itm) -> itm, false, 0).take(5)          
               ).replaceAll("\\_", " erros na "));

               //4) Quantidade de erros 404 por dia.
               System.out.println("quantidade de erros 404 por dia:\r\n" + String.join("\r\n", date404          
               .reduceByKey((a,b) -> a + b)               
               .map(item -> item._2 + " no dia " + item._1)
               .collect()
               ));                   
               
               //5) O total de bytes retornados.
               System.out.println("total de bytes: " + bytes.reduce((a,b) -> a+b));
                         
          } catch(Exception ex)  {
               throw ex;
          } finally {               
               sc.close();
          }           
     }
}          