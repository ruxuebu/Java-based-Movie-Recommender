import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class TopK_RecommenderGenerator {
	private static Map<Integer, PriorityQueue<String>> recommenderMap = new HashMap<>();
	
	/***
	 * Driver
	 * Find top k movie to recommend to user
	 * Output: user[i]: movie 
	 ***/
	public static void TopK_RecommenderGenerator_driver(String recommenderListPath, String recommenderResultPath, int k) throws Exception {
		Path rootPath = new Path(recommenderListPath);
		FileSystem fs = FileSystem.get(new Configuration());      		
		List<Path> filePaths = getFilesUnderFolder(fs, rootPath);
		
		for(Path path : filePaths) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = br.readLine();
			
			while (line != null) {
				String[] tokens = line.toString().trim().split("\t");
				int user = Integer.parseInt(tokens[0]);
				
				if(recommenderMap.containsKey(user)) {
					recommenderMap.get(user).add(tokens[1]);
					
					if(recommenderMap.get(user).size() > k) {
						recommenderMap.get(user).poll();				
					} 
				} else {
					PriorityQueue<String> pq = new PriorityQueue<String>(k + 1, new myComparator());
					pq.add(tokens[1]);
					recommenderMap.put(user, pq);
				}
				
				line = br.readLine();
			}
			
			br.close();
		}
		
        FileWriter writer;
        
        try {
            writer = new FileWriter(recommenderResultPath);
            
            for(Map.Entry<Integer, PriorityQueue<String>> entry : recommenderMap.entrySet()) {
            	int userId = entry.getKey();
            	PriorityQueue<String> pq = entry.getValue();
            	
            	while(!pq.isEmpty()) {
            		StringBuilder sb = new StringBuilder();
            		String movie = pq.poll().split(":")[0];
            		sb.append("user[").append(userId).append("]: ").append(movie).append("\n");
            		writer.write(sb.toString());
            		writer.flush();
            	}
            }
            
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
	}

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
}


class myComparator implements Comparator<String> {
	@Override
	public int compare(String str1, String str2) {
		String rating1 = str1.split(":")[1];
		String rating2 = str2.split(":")[1];
		return rating1.compareTo(rating2);
	}
} 
