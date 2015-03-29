import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {

	public static void main(String[] args) throws IOException {
		
		if (args.length < 3){
			System.out.println("Not enough arguments");
			System.exit(-1);
		}
		int NT = 1;
		String inputFileName, outputFileName; 
		
		NT = Integer.parseInt(args[0]);
		inputFileName = args[1]; outputFileName = args[2];
		
		BufferedReader inputFile = null;
		try {
			inputFile = new BufferedReader(new FileReader(inputFileName));
		} catch (IOException e) {
			System.out.println("Cannot open file " + inputFileName);
			System.exit(-2);
		}
		
		int D = 0, ND = 0;
		double X = 0;
		ArrayList<String> fileNames = null;
		try {
			D = Integer.parseInt(inputFile.readLine());
			X = Double.parseDouble(inputFile.readLine());
			ND = Integer.parseInt(inputFile.readLine());
			
			fileNames = new ArrayList<String>(ND);
			for (int i=0; i < ND; ++i)
				fileNames.add(inputFile.readLine());
			
		} catch (IOException e) {
			System.out.println("Error reading from file " + inputFileName);
			inputFile.close();
			System.exit(-3);
		} finally {
			inputFile.close();
		}
		
		// Map Stage
		ExecutorService mapWorkPool = Executors.newFixedThreadPool(NT);
		ArrayList<Future<MapResult>> mapFutures = new ArrayList<Future<MapResult>>();
		
		for (String fileName : fileNames){
			File file = new File(fileName);
			int size = (int) file.length();
			if (size > Integer.MAX_VALUE) {
				System.out.println("DIS FILE BE TU BIG FOR AN INT SIZE");
				System.exit(-13);
			}
			
			MapWorker mapWorker;
			Future<MapResult> fmr;
			for(int i=0; i < size; i += D) {
				mapWorker = new MapWorker(fileName, i, (i+D < size? D : size-i));
				fmr = mapWorkPool.submit(mapWorker);
				mapFutures.add(fmr);
			}
		}
		
		// Finished mapping stage
		mapWorkPool.shutdown();
		
		// Get the results from the mapping stage
		Map<String, List<MapResult>> mapResults = new HashMap<String, List<MapResult>>();
		
		for (Future<MapResult> f : mapFutures) {
			MapResult mr = null;
			try {
				mr = f.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
			String fileName = mr.getFileName();
			if (!mapResults.containsKey(fileName)){
				mapResults.put(fileName, new ArrayList<MapResult>());
			}
			mapResults.get(fileName).add(mr);
		}
		
		// Reduce Stage
		ExecutorService reduceWorkPool = Executors.newFixedThreadPool(NT);
		ArrayList<Future<ReduceResult>> reduceFutures = new ArrayList<Future<ReduceResult>>();
		
		for (String file : fileNames) {
			ReduceWorker reduceWorker = new ReduceWorker(file, mapResults.get(file));
			Future<ReduceResult> frr = reduceWorkPool.submit(reduceWorker);
			reduceFutures.add(frr);
		}
		
		// Finished Reduce Stage
		reduceWorkPool.shutdown();
		
		Map<String, ReduceResult> reduceResults = new HashMap<String, ReduceResult>();
		
		for (Future<ReduceResult> f : reduceFutures) {
			ReduceResult rr = null;
			try {
				rr = f.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
			String fileName = rr.getFileName();
			reduceResults.put(fileName, rr);
		}
		
		
		// Compare Stage
		ExecutorService compareWorkPool = Executors.newFixedThreadPool(NT);
		List<Future<CompareResult>> compareFutures = new ArrayList<Future<CompareResult>>();
		
		
		// Map to keep track for each file their currently compaired pairs
		Map<String,Set<String>> fileComparison = new HashMap<String,Set<String>>();
		
		for (Map.Entry<String, ReduceResult> res1 : reduceResults.entrySet()) {			
			String key1 = res1.getKey();
			fileComparison.put(key1, new HashSet<String>());

			// We don't want to compare a document with itself
			fileComparison.get(key1).add(key1);
			
			for (Map.Entry<String, ReduceResult> res2 : reduceResults.entrySet()) {
				String key2 = res2.getKey();
				// Check if we didn't already compaired these 2 files
				if ((fileComparison.get(key2) != null) && (!fileComparison.get(key2).contains(key1)))
				{
					CompareWorker cw = new CompareWorker(res1.getValue(), res2.getValue());
					Future<CompareResult> fcr = compareWorkPool.submit(cw);
					compareFutures.add(fcr);
					
					fileComparison.get(key1).add(key2);
				}
				
			}
		}
		
		// Finished Compare Stage
		compareWorkPool.shutdown();
		
		// Sort in desc. order pairs of documents by their similarity
		SortedSet<CompareResult> pairSet = Collections.synchronizedSortedSet(new TreeSet<CompareResult>());
		
		for (Future<CompareResult> f : compareFutures){
			CompareResult cr = null;
			try {
				cr = f.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
			double sim = cr.getSimilarity();
			if (sim > X){
				pairSet.add(cr);
			}
		}
		
		// Write results to output file
		BufferedWriter outputFile = null;
		try {
			outputFile = new BufferedWriter(new FileWriter(outputFileName));
		} catch (IOException e) {
			System.out.println("Cannot open file " + outputFileName);
			System.exit(-2);
		}
		
		Iterator<CompareResult> iter = pairSet.iterator();
		while (iter.hasNext()) {
			CompareResult cr = iter.next();
			String fileName1 = cr.getFileName1();
			String fileName2 = cr.getFileName2();
			String displaySim = new DecimalFormat("#.####").format(cr.getSimilarity());
			
			if (Main.compareBecauseStupidChecker(fileName1, fileName2) < 0){
				outputFile.write(fileName1 + ";" + fileName2 + ";" + displaySim + "\n");
			} else {
				outputFile.write(fileName2 + ";" + fileName1 + ";" + displaySim + "\n");
			}
			
			
		}
		
		outputFile.close();
	}
	
	public static int compareBecauseStupidChecker(String s1, String s2){
		int len1 = s1.length();
		int len2 = s2.length();
		
		if (s1.compareTo(s2) < 0){
			// 20mb < 5mb as String but 5mb < 20mb as int
			if (len1 > len2)
				return 1;
			else
				return -1;
		} else {
			// 5mb > 20mb as String but 5mb < 20mb as int
			if (len1 < len2)
				return -1;
			else
				return 1;
		}
		
	}

}
