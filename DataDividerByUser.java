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


public class DataDividerByUser {
	/***
	 * map method
	 * Input: user,movie_id,rating
	 * 
	 ***/
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] user_movie_rating = value.toString().trim().split(",");
			int userId = Integer.parseInt(user_movie_rating[0]);
			String movieId = user_movie_rating[1];
			String rating = user_movie_rating[2];			
			context.write(new IntWritable(userId), new Text(movieId + ":" + rating));
		}
	}

	
	/***
	 * reduce method
	 * Input: key = userId, value = <movieId:rating, moveId:rating...>; 
	 * reducer will collection the value by the same key.
	 * 
	 ***/
	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder();
			
			while(values.iterator().hasNext()) {
				builder.append("," + values.iterator().next().toString());
			}
			
			context.write(key, new Text(builder.toString().replaceFirst(",", "")));
		}
	}
	
	
	/***
	 * Driver
	 * 
	 ***/
	public void DataDividerByUser_dirver(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);
		job.setJarByClass(DataDividerByUser.class);
		
		job.setInputFormatClass(TextInputFormat.class);  
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.setInputPaths(job, new Path(input));
		TextOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}
}
