import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;





public class Multiplication {		
	/***
	 * map method
	 * 
	 ***/
	public static class MultiplicationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		Map<Integer, List<MovieRelation>> movieRelationMap = new HashMap<Integer, List<MovieRelation>>();
		Map<Integer, Integer> denominator = new HashMap<>();
				
		/**
		 * setup and get co-occurence matrix from file
		 * Input: movieA : movieB \t relation
		 * */
		@Override
		protected void setup(Context context) throws IOException {		
			Configuration conf = context.getConfiguration();
			String root = "hdfs:/user/root/" + conf.get("coOccurrencePath");
			Path rootPath = new Path(root);

			FileSystem fs = FileSystem.get(conf);      		
			List<Path> filePaths = getFilesUnderFolder(fs, rootPath);
			
			for (Path path : filePaths) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line = br.readLine();
				
				while (line != null) {
					String[] tokens = line.toString().trim().split("\t");
					String[] movies = tokens[0].split(":");
					
					int movie1 = Integer.parseInt(movies[0]);
					int movie2 = Integer.parseInt(movies[1]);
					int relation = Integer.parseInt(tokens[1]);
					
					MovieRelation movieRelation = new MovieRelation(movie1, movie2, relation);
					
					if (movieRelationMap.containsKey(movie1)) {
						movieRelationMap.get(movie1).add(movieRelation);
					} else {
						List<MovieRelation> list = new ArrayList<>();
						list.add(movieRelation);
						movieRelationMap.put(movie1, list);
					}
					
					line = br.readLine();
				}
				
				br.close();
			}
						
			for (Map.Entry<Integer, List<MovieRelation>> entry : movieRelationMap.entrySet()) {
				int sum = 0;
				
				for (MovieRelation relation : entry.getValue()) {
					sum += relation.getRelation();
				}
				
				denominator.put(entry.getKey(), sum);
			}
		}
		
		// get all path of files under the given folderPath
		private static List<Path> getFilesUnderFolder(FileSystem fs, Path folderPath) throws IOException {  
	        List<Path> paths = new ArrayList<Path>();  
	        
	        if (fs.exists(folderPath)) {  
	            FileStatus[] fileStatus = fs.listStatus(folderPath);  
	            
	            for (int i = 0; i < fileStatus.length; i++) {   
	                Path oneFilePath = fileStatus[i].getPath();   
	                paths.add(oneFilePath);          
	            }  
	        }
	        
	        return paths;  
	    }  
		
				
		/***
		 * Input:  user,movie,rating
		 * Output: user: movie score
		 * 
		 ***/
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] tokens = value.toString().trim().split(",");
			int user = Integer.parseInt(tokens[0]);
			int movie = Integer.parseInt(tokens[1]);
			double rating = Double.parseDouble(tokens[2]);
						
			for(MovieRelation relation : movieRelationMap.get(movie)) {
				double score = rating * relation.getRelation();				
				score = score / denominator.get(relation.getMovie2());    				
				DecimalFormat df = new DecimalFormat("#.00");             
				score = Double.valueOf(df.format(score));
				context.write(new Text(user + ":" + relation.getMovie2()), new DoubleWritable(score));   
			}
		}
	}
	

	/***
	 * reduce method
	 * Input: user:movie \t score
	 * 
	 ***/
	public static class MultiplicationReducer extends Reducer<Text, DoubleWritable, IntWritable, Text> {
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			
			while(values.iterator().hasNext()){
				sum += values.iterator().next().get();
			}
			
			String[] tokens = key.toString().split(":");
			int user = Integer.parseInt(tokens[0]);
			context.write(new IntWritable(user), new Text(tokens[1] + ":" + sum));
		}
	}
	
		
	/***
	 * Driver
	 * 
	 ***/
	public void Multiplication_dirver(String input, String output, String coOccurrencePath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("coOccurrencePath", coOccurrencePath);
		
		Job job = Job.getInstance(conf);
		job.setMapperClass(MultiplicationMapper.class);
		job.setReducerClass(MultiplicationReducer.class);
		job.setJarByClass(Multiplication.class);
		job.setNumReduceTasks(3);                         
		
		job.setInputFormatClass(TextInputFormat.class);   
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);    
		job.setMapOutputValueClass(DoubleWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(input));  
		TextOutputFormat.setOutputPath(job, new Path(output)); 
		
		job.waitForCompletion(true);
	}
}
