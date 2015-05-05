import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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

			 String[] result = values.toString().split("\t");
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
			if (num_outlinks <= 0 ) {
				return;
			}
			double out_score = pagescore / num_outlinks;
			for (int i = 2; i < result.length ; i++ ) {
				out_str = "SCORE" + kSeparator + out_score;
				context.write(new Text(result[i]), new Text(out_str));
			}
		}
	}


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
			String out_key = page_sum + " ";
			if (link_str.isEmpty()) {
				link_str = key.toString();
			}
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
