package com.hadoop.run;

import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * ./hadoop jar app-hadoop-0.1.0-jar-with-dependencies.jar wordcount input/word.txt output
 * ./hdfs dfs -cat output2/part-r-00000
 * */
public class WordCount {
	
	public static void main(String [] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf , args).getRemainingArgs();
		
		System.out.println(otherArgs[0]);
		System.out.println(otherArgs[1]);
		System.out.println(otherArgs[2]);
		if(otherArgs.length !=2) {
			System.err.println("Usage:wordcount <in> <out>");
			//System.exit(2);
		}
		
		Job job = new Job(conf , "wordcount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class Map extends Mapper <Object , Text , Text , IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key , Text value , Context context) 
			throws IOException , InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word , one);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text , IntWritable , Text , IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key , Iterable<IntWritable> values , 
				Context context) throws IOException , InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum+=val.get();
			}
			
			result.set(sum);
			context.write(key , result);
		}
	}
}