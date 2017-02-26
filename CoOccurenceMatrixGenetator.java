import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CoOccurenceMatrixGenetator {
	/***
	 * map method
	 * Input: userId \t movie1:rating, movie2:rating...
	 * Output: key = movie1 : movie2,  value = 1...
	 * 
	 ***/	
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] user_movieRatings = line.split("\t"); 
			String user = user_movieRatings[0];
			String[] movie_ratings = user_movieRatings[1].split(",");
			
			for (int i = 0; i < movie_ratings.length; i++) {
				String movie1 = movie_ratings[i].trim().split(":")[0];
				
				for (int j = 0; j < movie_ratings.length; j++) {
					String movie2 = movie_ratings[j].trim().split(":")[0];
					context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
				}
			}
		}
	}

	
	/***
	 * reduce method
	 * Input key = movie1:movie2,  value = iterable<1,1,1>
	 * 
	 ***/
	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			while (values.iterator().hasNext()) {
				sum += Integer.parseInt(values.iterator().next().toString());
			}
			
			context.write(key, new IntWritable(sum));
		}
	}
	
	
	/***
	 * Driver
	 * 
	 ***/
	public void CoOccurenceMatrixGenetator_driver(String input, String output) throws Exception {
		Job job = Job.getInstance();
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);
		job.setJarByClass(CoOccurenceMatrixGenetator.class);
		job.setNumReduceTasks(3);  
		
		job.setInputFormatClass(TextInputFormat.class);   
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(input));  
		TextOutputFormat.setOutputPath(job, new Path(output)); 
		
		job.waitForCompletion(true);   
	}
}
