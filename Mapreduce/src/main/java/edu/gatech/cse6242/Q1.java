//Author: Manish Puri
//Note: Portions of the code have been completed using documentation found at:
//hadoop.apache.org/docs/r2.7.4 and discussions on Piazza

package edu.gatech.cse6242;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q1 {

 
  static class MapJob extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
  		public void map(LongWritable key, Text val, Context cont) throws InterruptedException, IOException {
  					
  					String[] data = (val.toString()).split("\t");
  					
  					//getting values for tgt and weight
  					IntWritable tgt = new IntWritable(Integer.parseInt(data[1]));
  					IntWritable weight = new IntWritable(Integer.parseInt(data[2]));
  					//mapjob result	
  					cont.write(tgt, weight);
  				}
  			}

  static class ReduceJob extends Reducer<IntWritable,IntWritable,Text,IntWritable>{
  
  		public void reduce(IntWritable key, Iterable<IntWritable> val, Context cont) throws InterruptedException, IOException{

  			int result = 0;
  			// iterating through values
  			for (IntWritable x: val)
  				result+=x.get();
  			
  			IntWritable reduceResult = new IntWritable(result);
  			
  			String key1 = key.toString();
  			Text key_text = new Text(key1);
  			//reduce result
  			cont.write(key_text, reduceResult);
  			
  				}
  		}
  
  //main method to run program
  public static void main(String[] args) throws Exception {
	    
	  	Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Q1");
	    job.setJarByClass(Q1.class);
	    
	    //setting mapper and reducer class as well as output files
	    job.setMapperClass(MapJob.class);
	    job.setReducerClass(ReduceJob.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);

	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}

