<<<<<<< HEAD
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
=======

import java.io.IOException;
import java.util.*;       

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.*;
>>>>>>> 5313ca981288e13e54033498ec0040d3895d3c6d
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
<<<<<<< HEAD
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author Poonkodi
 *
 */
public class PageRank {
	static String kSeparator = "<>";

	public static class PageRankMapper extends Mapper<LongWritable , Text,  Text, Text> {

		public void map(LongWritable key, Text values, Context context) 
				throws IOException, InterruptedException {

			 String[] result = values.toString().split("\\s");
			 for (int x=0; x < result.length ; x++)
			      System.out.println(result[x]);
			
			if (result.length < 2) {
				return;
			}

			double pagescore = Double.parseDouble(result[0]);

			String myurl=result[1];

			String v = values.toString(); 
			String outlinks = v.substring(v.indexOf('\t')+1);
			String out_str = "LINKS" + kSeparator + outlinks;
			context.write(new Text(myurl), new Text(out_str) );

			int num_outlinks = result.length - 2;
=======

@SuppressWarnings("deprecation")
public class PageRank {

	//Given Links - 1.0,A,B,C,D
	//              ^   ^    B,C,D are outlinks.
	//				PR  URL  
	//					B(C,D),C(D)
	//					D(A)
	//		Map: out(B,1.00)
	//			 out(c,0.5)
	//			 out(D,0.5)
	//			 out(D,0.5)
	//			 out(A,0.5)
	//			 
	//		Reducer: A->1.00
	//				 B-> 1.00
	//				 C->1.5
	//				 D->2.0


	public static class Map extends Mapper<Text , Text,  Text, Double> {

		private final static IntWritable link_score=new IntWritable(); 
		private Text word = new Text();

		public void map(Text key, Text values, Context context) 
				throws IOException, InterruptedException {

			List<String> items = Arrays.asList(values.toString().split("\\s*,\\s*"));
			if (items.size() < 2) {
				return;
			}

			double pagescore = Double.parseDouble(items.get(0));
			String myurl=items.get(1);
			//context.write(items);
			int num_outlinks = items.size() - 2;
>>>>>>> 5313ca981288e13e54033498ec0040d3895d3c6d
			if (num_outlinks <= 0 ) {
				return;
			}
			double out_score = pagescore / num_outlinks;
<<<<<<< HEAD
			for (int i = 2; i < result.length ; i++ ) {
				out_str = "SCORE" + kSeparator + out_score;
				context.write(new Text(result[i]), new Text(out_str));
=======
			for (int i = 2; i < items.size() ; i++ ) {
				context.write(new Text(items.get(i)), out_score);
>>>>>>> 5313ca981288e13e54033498ec0040d3895d3c6d
			}
		}
	}

<<<<<<< HEAD

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			String link_str = "";

			double page_sum = 0.0;
			Iterator<Text> val_iter = value.iterator();
			while (val_iter.hasNext()) {
				String v = val_iter.next().toString();		
				
				System.out.println("ReducerInput: k:" + key + "  ; v :" + v);
				 String[] result = v.split(kSeparator);
				 for (int x=0; x < result.length ; x++) {
				      System.out.println("Reducer Args : " + result[x]);
				 }
				
				
				
				if (result.length != 2) continue;

				System.out.println("tag:" + result[0]);

				if (result[0].equals("LINKS")) {
					link_str = result[1];
				} else if (result[0].equals("SCORE")) {
					double score = Double.parseDouble(result[1]);
					page_sum += score;
				}
			}
			String out_key = page_sum + "";
			
			System.out.println("ReducerOutput: k:"+ out_key + " ; v:" + link_str);
			
			context.write(new Text(out_key) , new Text(link_str));
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void RunPageRankIteration(Configuration conf, String run_label, 
			String input_path, String output_path) throws Exception {
		System.out.println("Starting PR iteration  : " + run_label);
		Job job = new Job(conf, run_label);
		job.setJarByClass((Class)PageRank.class);
		job.setMapperClass((Class)PageRankMapper.class);

		job.setReducerClass((Class)PageRankReducer.class);
		
		job.setOutputKeyClass((Class)Text.class);
		job.setOutputValueClass((Class)Text.class);
				
	    job.setInputFormatClass((Class)TextInputFormat.class);
	    job.setOutputFormatClass((Class)TextOutputFormat.class);
		
		FileInputFormat.setInputPaths((Job)job, (Path)new Path(input_path));
	    FileOutputFormat.setOutputPath((Job)job, (Path)new Path(output_path));
	    
	    job.waitForCompletion(true);
	    
	    System.out.println("Completing PR iteration  : " + run_label);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: PageRank input_path output_prefix num_iterations");
			System.exit(2);
		}
		
		String input_path = otherArgs[0];
		String output_prefix = otherArgs[1] ;
		int num_iters = Integer.parseInt(otherArgs[2]);
		
		for (int i = 0; i < num_iters; i++) {
			String out_path = output_prefix + "iter_" + i;
			RunPageRankIteration(conf, "PR_iter_" + i, input_path, out_path);
			input_path = out_path + "/";
			
		}
		
		System.exit(0);
	}
	
}
=======
	public static class Reduce extends Reducer<Text, Iterator<Double>, Text, Double> 
	{
		public void reduce(Text key, Iterator<Double> value, Context context) 
				throws IOException, InterruptedException {
			double page_sum = 0.0;
			while (value.hasNext()) {
				page_sum += value.next();
			}
			context.write(key, page_sum);
			//context.write(key,page_sum,outlinks);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "pagerank");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
>>>>>>> 5313ca981288e13e54033498ec0040d3895d3c6d
