import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 

public class RecommenderListGenerator {
	/***
	 * map method
	 * filter out the movies which user has watched before
	 * Input: user,movie_id,rating
	 * 
	 ***/
	public static class RecommenderListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {	
		Map<Integer, List<Integer>> watchHistoryMap = new HashMap<>();
		
		@Override
		/***
		 * read movie watch history, these information can be stored in database
		 * user,movie,rating
		 * 
		 ***/
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String filePath = "hdfs:/user/root/" + conf.get("watchHistoryPath");
			Path pt = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			
			while (line != null) {
				String[] tokens = line.split(",");
				int user = Integer.parseInt(tokens[0]);
				int movie = Integer.parseInt(tokens[1]);
				
				if(watchHistoryMap.containsKey(user)) {
					watchHistoryMap.get(user).add(movie);
				} else {
					List<Integer> list = new ArrayList<Integer>();
					list.add(movie);
					watchHistoryMap.put(user, list);
				}
				
				line = br.readLine();
			}
			
			br.close();
		}
		
		
		/***
		 * remove the movie user has seen before
		 * Input: user \t movie:rating
		 * 
		 ***/
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			int user = Integer.parseInt(tokens[0]);
			int movie = Integer.parseInt(tokens[1].split(":")[0]);
			
			if(!watchHistoryMap.get(user).contains(movie)) {
				context.write(new IntWritable(user), new Text(movie + ":" + tokens[1].split(":")[1]));
			}
		}
	}
	
	
	/***
	 * reduce method
	 * 
	 ***/
	public static class RecommenderListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		Map<Integer, String> movieTitleMap = new HashMap<>();
		
		/***
		 * read <movie_id, movie_title>, this table can be stored in database
		 * read movie title from file
		 * 
		 ***/
		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String filePath = "hdfs:/user/root/" + conf.get("movieTitlesPath");
			Path pt = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			
			while(line != null) {
				String[] tokens = line.split(",");
				int movie_id = Integer.parseInt(tokens[0]);
				movieTitleMap.put(movie_id, tokens[1]);
				line = br.readLine();
			}
			
			br.close();
		}
		
		
		/***
		 * match movie_name to movie_id
		 * Input: user \t movie_id:rating
		 * Output: movie_name
		 * 
		 ***/
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			while(values.iterator().hasNext()) {
				String cur = values.iterator().next().toString();
				int movie_id = Integer.parseInt(cur.split(":")[0]);
				String rating = cur.split(":")[1];
				
				if(!movieTitleMap.get(movie_id).equals(null)) {
					context.write(key, new Text(movieTitleMap.get(movie_id) + ":" + rating));
				}
			}
		}
	}
	
	
	/***
	 * Driver
	 * 
	 ***/
	public void RecommenderListGenerator_driver(String watchHistoryPath, String movieTitlesPath, String input, String output) throws Exception {
		Configuration conf = new Configuration();
		conf.set("watchHistoryPath", watchHistoryPath);
		conf.set("movieTitlesPath", movieTitlesPath);
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(RecommenderListGeneratorMapper.class);
		job.setReducerClass(RecommenderListGeneratorReducer.class);
		job.setJarByClass(RecommenderListGenerator.class);
		job.setNumReduceTasks(3);  
		
		job.setInputFormatClass(TextInputFormat.class);   
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(IntWritable.class);    
		job.setMapOutputValueClass(Text.class);
				
		TextInputFormat.setInputPaths(job, new Path(input));    
		TextOutputFormat.setOutputPath(job, new Path(output));  
		
		job.waitForCompletion(true);
	}
}
