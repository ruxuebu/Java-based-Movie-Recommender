import org.apache.hadoop.conf.Configuration;

public class Driver {
	/***
	 * main method
	 * Input: two files, watchHistory and movieTitle
	 * Output will be saved in the file of recommenderResult 
	 * 
	 ***/
	public static void main(String[] args) throws Exception {
		if (args.length < 9) {
			new Exception("Please input: <watchHistory dir> <output1 dir> <output2 dir> <output3 dir> <output4 dir> <watchHistory dir> <movieTitles dir> <recommenderResult dir> <int k>\n");
			return ;
		}
		
		Configuration conf = new Configuration();		
		conf.set("coOccurrencePath", args[2]);
		conf.set("watchHistoryPath", args[0]);
		conf.set("movieTitlesPath", args[4]);
		
		DataDividerByUser dataDivider = new DataDividerByUser();
		CoOccurenceMatrixGenetator cGenetator = new CoOccurenceMatrixGenetator();
		Multiplication multiplication = new Multiplication();
		RecommenderListGenerator rGenerator = new RecommenderListGenerator(); 
		TopK_RecommenderGenerator topkGenerator = new TopK_RecommenderGenerator();
		
		String dataDividerInput = args[0];
		String dataDividerOutput = args[1];
		
		String CoOccurenceMatrixGenetatorInput = args[1];
		String CoOccurenceMatrixGenetatorOutput = args[2];
		
		String MultiplicationInput = args[0];
		String MultiplicationOutput = args[3];
		String coOccurrencePath = args[2];
		
		String RecommenderListGeneratorInput = args[3];
		String RecommenderListGeneratorOutput = args[4];
		
		String watchHistory = args[5];
		String movieTitles = args[6];
		
		String recommenderListPath = args[4];
		String recommenderResultPath = args[7];
		int k = Integer.parseInt(args[8]);
		
		dataDivider.DataDividerByUser_dirver(dataDividerInput, dataDividerOutput);
		cGenetator.CoOccurenceMatrixGenetator_driver(CoOccurenceMatrixGenetatorInput, CoOccurenceMatrixGenetatorOutput);
		multiplication.Multiplication_dirver(MultiplicationInput, MultiplicationOutput, coOccurrencePath);
		rGenerator.RecommenderListGenerator_driver(watchHistory, movieTitles, RecommenderListGeneratorInput, RecommenderListGeneratorOutput);
		topkGenerator.TopK_RecommenderGenerator_driver(recommenderListPath, recommenderResultPath, k);
	}
}
