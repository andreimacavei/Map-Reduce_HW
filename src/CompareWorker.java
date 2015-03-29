import java.util.Map;
import java.util.concurrent.Callable;

class CompareResult implements Comparable<CompareResult>{
	private double similarity;
	private String fileName1, fileName2;
	
	public CompareResult(String fileName1, String fileName2) {
		this.similarity = 0;
		this.fileName1 = fileName1;
		this.fileName2 = fileName2;
	}
	
	public double getSimilarity() {
		return similarity;
	}
	
	public void setSimilarity(double similarity){
		this.similarity = similarity;
	}
	public String getFileName1() {
		return fileName1;
	}
	public String getFileName2() {
		return fileName2;
	}
	
	@Override
	public int compareTo(CompareResult o) {
		if (this.similarity == o.similarity)
			return 0;
		return (this.similarity < o.similarity ? 1 : -1);
	}
}


public class CompareWorker implements Callable<CompareResult> {

	private ReduceResult file1Data, file2Data;
	
	public CompareWorker(ReduceResult file1Data, ReduceResult file2Data){
		this.file1Data = file1Data;
		this.file2Data = file2Data;
	}

	@Override
	public CompareResult call() throws Exception {
		
		// Get the total number of words for each file
		int file1Count = 0, file2Count = 0;
		
		for(Map.Entry<String, Integer> e : file1Data.getIndexedFile().entrySet())
			file1Count += e.getValue();
		for(Map.Entry<String, Integer> e : file2Data.getIndexedFile().entrySet())
			file2Count += e.getValue();
		
		/* Calculate similarity using formula : 
		   sim(file1, file2) = sum(f(t,file1) * f(t,file2)) [%] */
		double sim = 0;
		
		CompareResult compareResult = new CompareResult(file1Data.getFileName(), file2Data.getFileName());
		for (Map.Entry<String, Integer> e : file1Data.getIndexedFile().entrySet()){
			Integer f2 = file2Data.getIndexedFile().get(e.getKey());
			if (f2 != null){
				double f1 = (double)e.getValue() / file1Count;
				sim += f1 * ((double)f2 / file2Count);
				
			}
			
		}
		
		compareResult.setSimilarity(sim * 100);
		return compareResult;
	}
	
	
	
}
