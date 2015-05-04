import java.util.List;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

/*
 * The program loads the inverted index for all the tokens and page rank of the URLS in memory.
 * On top of this search is performed
 * 
 */
public class SearchTerm {
	// Contains URL as key and pagerank as value
	Map<String, Double> pagerank_map;
	// Contains term as key and top n urls as value
	Map<String, ArrayList<String>> document_index;

	SearchTerm() {
		document_index = new HashMap<String, ArrayList<String>>();
		pagerank_map = new HashMap<String, Double>();
	}

	// Read file from pagerank.
	// And load it to the pagerank_map.
	public void loadPageRank() {
		Path pt = new Path("/test/dirs/output/part-r-00000");
		FileSystem fs;
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path(
					"/Users/aparna/install/hadoop-0.23.9/etc/hadoop/core-site.xml"));
			conf.addResource(new Path(
					"/Users/aparna/install/hadoop-0.23.9/etc/hadoop/hdfs-site.xml"));
			fs = FileSystem.get(conf);
			Scanner scanner = new Scanner(fs.open(pt));
			while (scanner.hasNext()) {
				String line = scanner.nextLine();
				String[] values = line.split("\t");
				double page_score = Double.parseDouble(values[0].trim());
				String url = values[1].trim();
				pagerank_map.put(url, page_score);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 * Looks up pagerank for all urls. And sorts by decreasing order of
	 * pagerank. If PR not found, assigns zero pagerank.
	 * 
	 * <1, url1>, <2, url2> , <1, url3>, <1, url4>
	 */
	public ArrayList<String> sortDocsByPageRank(ArrayList<String> input_docs) {
		Map<Double, ArrayList<String>> doc_map = new TreeMap<Double, ArrayList<String>>();

		for (String url : input_docs) {
			double pr = pagerank_map.containsKey(url) ? pagerank_map.get(url)
					: 0.0;
			if (doc_map.containsKey(pr)) {
				doc_map.get(pr).add(url);
			} else {
				ArrayList<String> nlist = new ArrayList<String>();
				nlist.add(url);
				doc_map.put(pr, nlist);
			}
		}
		ArrayList<String> output_sorted_urls = new ArrayList<String>();
		for (ArrayList<String> v : doc_map.values()) {
			output_sorted_urls.addAll(v);
		}
		return output_sorted_urls;
	}

	/*
	 * Reads the index and loads it to memory.
	 */
	public void loadIndex() {
		TreeMap<String, Integer> tmap = null;
		Path pt = new Path("/test/dirs/output/part-r-00000");
		FileSystem fs;
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path(
					"/Users/aparna/install/hadoop-0.23.9/etc/hadoop/core-site.xml"));
			conf.addResource(new Path(
					"/Users/aparna/install/hadoop-0.23.9/etc/hadoop/hdfs-site.xml"));
			fs = FileSystem.get(conf);
			Scanner scanner = new Scanner(fs.open(pt));
			while (scanner.hasNext()) {
				String line = scanner.nextLine();
				String[] values = line.split("\\{");

				String token = values[0].trim().toLowerCase();

				String docs = values[1].substring(0, values[1].length() - 1);

				ArrayList<String> match_docs = new ArrayList<String>();

				String[] documents = docs.split(",");
				for (String document : documents) {
					String[] values1 = document.split("=");
					String url = values1[0].trim();
					match_docs.add(url);
				}

				ArrayList<String> pr_sorted = sortDocsByPageRank(match_docs);
				document_index.put(token, pr_sorted);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args) {

		SearchTerm index = new SearchTerm();
		index.loadPageRank();
		index.loadIndex();
		List<String> result_urls;
		Scanner in = new Scanner(System.in);
		while (true) {

			System.out.println("Enter query > ");
			String query = in.nextLine().trim().toLowerCase();
			result_urls = index.searchIndex(query);
			index.displayResults(query, result_urls);
		}
	}

	private void displayResults(String query, List<String> result_urls) {
		// TODO Auto-generated method stub
		System.out.println("\nSearch results for " + query);

		for (String url : result_urls)
			System.out.println("http://en.wikipedia.org/wiki/" + url);

		System.out.println("\n");

	}

	//Fetch the URLs from document index
	private List<String> searchIndex(String raw_query) {

		// process query. Do stemming here if needed.
		String processed_query = raw_query;
		// list of strings
		if (document_index.containsKey(processed_query)) {

			 return document_index.get(processed_query);
		} else
			return null;
	}

}
