import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;


class MapResult {
	private String fileName;
	private Map<String, Integer> fragmentCount;
	
	public MapResult(String fileName) {
		this.fileName = fileName;
		fragmentCount = new HashMap<String, Integer>();
	}

	public void addToTable(String nextToken) {
		int count = 0;
		if (fragmentCount.containsKey(nextToken)) {
			count = fragmentCount.get(nextToken) + 1;
			
		} else {
			count++;
		}
		fragmentCount.put(nextToken, count);
	}

	public String getFileName() {
		return fileName;
	}

	public Set<String> getKeys() {
		return fragmentCount.keySet();
	}
	
	public int getKeyCount(String key) {
		int count = 0;
		if (fragmentCount.containsKey(key))
			count = fragmentCount.get(key);
		
		return count;
	}
}

public class MapWorker implements Callable<MapResult>{

	private String fileName;
	private int start;
	private int size;
	private final static String regex = " ;:/?~\\.,><~`[]{}()!@#$%^&-_+\'=*\"|\t\n";
	
	public MapWorker(String fileName, int start, int size) {
		this.fileName = fileName;
		this.start = start;
		this.size = size;
	}
	
	@Override
	public MapResult call() throws Exception {
		
		RandomAccessFile rfile = new RandomAccessFile(fileName, "r");
		int buff = 50;
		// We allocate buff elements if needed to increase finish
		byte [] fragment = new byte[size + buff]; 
		

		try {
			// Seek at start-1 position to check if fragment starts in middle of a word
			if (start != 0)
				rfile.seek(start-1);
			else 
				rfile.seek(start);
			rfile.read(fragment, 0, fragment.length);
			
		} catch (IOException e) {
			rfile.close();
			throw new Exception("Cannot read " + rfile);
		}
		
		
		// Check if fragment starts in the middle of a word
		int startIndex = 0;
		if ((Character.isDigit(fragment[0]) || Character.isLetter(fragment[0])) 
				&& start != 0){
//		if (!isDelimiter(fragment[0]) && start != 0){
//			while (!isDelimiter(fragment[startIndex]) && startIndex < size) {
			while (fragment[startIndex] != ' ' && startIndex < size) {
				startIndex++;
			}
		} else{
			// This check is because we previous used seek(start-1) if fragment was not at start of the file
			if (start != 0)
				startIndex++;
		}
		
		int endIndex = size;
		// Check if the fragment ends in the middle of a word
		while ((Character.isDigit(fragment[endIndex]) || Character.isLetter(fragment[endIndex]))
					&& endIndex < fragment.length-1){
//		while (!isDelimiter(fragment[endIndex]) && endIndex < fragment.length-1) {
			endIndex++;
		}
		
		String contents = new String(Arrays.copyOfRange(fragment, startIndex, endIndex), "US-ASCII");
		
		
		contents = contents.toLowerCase().replaceAll("[^0-9a-z ]", " ");
//		System.out.println(contents);
		
		StringTokenizer tok = new StringTokenizer(contents, " ");
		
		MapResult mapResult = new MapResult(fileName);
		
		while(tok.hasMoreElements()) {
			mapResult.addToTable(tok.nextToken());
		}
		rfile.close();
		return mapResult;
	}
	
	private boolean isDelimiter(byte c){
		if (regex.contains(""+c)){
			
			return true;
		}
		return false;
	}
}
