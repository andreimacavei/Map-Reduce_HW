import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;


class ReduceResult {
	private String fileName;
	private Map<String,Integer> indexedFile;
	
	public ReduceResult(String fileName){
		this.fileName = fileName;
		indexedFile = new HashMap<String,Integer>();
	}

	public String getFileName() {
		return fileName;
	}
	
	public Map<String,Integer> getIndexedFile() {
		return indexedFile;
	}
	
	public void putWord(String key, int count) {
		this.indexedFile.put(key, count);
	}
}

public class ReduceWorker implements Callable<ReduceResult> {

	private String fileName;
	private List<MapResult> mapResults;
	
	public ReduceWorker(String fileName, List<MapResult> mapResults){
		this.fileName = fileName;
		this.mapResults = mapResults;
	}
	
	
	@Override
	public ReduceResult call() throws Exception {
		
		/* Combine the Hash Maps of each fragment of a file 
		   into a single Hash Map */
		// Create a set with all keys/words from the file
		Set<String> keys = new HashSet<String>();
		for (MapResult mr : mapResults) {
			keys.addAll(mr.getKeys());
		}
		
		// Get the total number for each key/word in the entire file
		ReduceResult reduceResult = new ReduceResult(fileName);
		int count;
		for (String key : keys)	{
			count = 0;
			for (MapResult mr : mapResults) {
				count += mr.getKeyCount(key);
			}
			reduceResult.putWord(key, count);
		}
		
		return reduceResult;
	}

}
