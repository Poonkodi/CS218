import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import org.tartarus.snowball.ext.englishStemmer;

/**
 * @author Poonkodi Ponnambalam
 * The program loads the inverted index for all the tokens and
 *  page rank of the URLS in memory.
 * On top of this search is performed
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

	/**
	 * Read file from pagerank.
	 * And load it to the pagerank_map.
	 * @throws FileNotFoundException
	 */
	public void loadPageRank() throws FileNotFoundException {
		String pageRankFile="/Users/vinodh/abitha/Spring2015/cloud-comp/PRoutput";
		Scanner scanner = new Scanner(new File(pageRankFile));
		while (scanner.hasNext()) {
			String line = scanner.nextLine();
			String[] values = line.split("\t");
			double page_score = Double.parseDouble(values[0].trim());
			String url = values[1].trim();
			pagerank_map.put(url, page_score);
		}
	} 

	/**
	 * Looks up pagerank for all urls. And sorts by decreasing order of
	 * pagerank. If PR not found, assigns zero pagerank.
	 * <1, url1>, <2, url2>, <1, url3>, <1, url4>
	 */
	public ArrayList<String> sortDocsByPageRank(ArrayList<String> input_docs) {
		Map<Double, ArrayList<String>> doc_map = new TreeMap<Double, ArrayList<String>>();
		for (String url : input_docs) {
			double pr = pagerank_map.containsKey(url) ? -1.0*pagerank_map.get(url)
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

	/**
	 * Reads the index and loads it to memory.
	 * @throws FileNotFoundException
	 */
	public void loadIndex() throws FileNotFoundException {
		TreeMap<String, Integer> tmap = null;
		String invertedIndexFile="/Users/vinodh/abitha/Spring2015/cloud-comp/IIOutput";
		Scanner scanner = new Scanner(new File(invertedIndexFile));
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
	}


	/**
	 * Main file.
	 */
	public static void main(String[] args) throws FileNotFoundException {
		SearchTerm index = new SearchTerm();
		index.loadPageRank();
		index.loadIndex();
		List<String> result_urls;
		Scanner in = new Scanner(System.in);
		while (true) {
			System.out.println("Enter query > ");
			String query = in.nextLine().trim().toLowerCase();
			englishStemmer stemmer = new englishStemmer();
			String stemmedQuery = "";
			stemmer.setCurrent(query);
			if(stemmer.stem())
				stemmedQuery = stemmer.getCurrent();
			result_urls = index.searchIndex(stemmedQuery);
			index.displayResults(query, result_urls);
		}
	}

	/**
	 * Displays results of the retreived documents.
	 * @param query
	 * @param result_urls
	 */
	private void displayResults(String query, List<String> result_urls) {
		System.out.println("Search results for " + query);
		if (result_urls == null || result_urls.size() == 0) {
			System.out.println("No results found.");
			return;
		}
		for (String url : result_urls)
		{
			String doc=url.replaceAll(" ", "_");
			System.out.println("http://simple.wikipedia.org/wiki/" + doc);
		}
		System.out.println("\n");
	}

	/**
	 * Fetch the URLs from document index
	 * @param raw_query
	 * @return the list of URLS for the query
	 */
	private List<String> searchIndex(String raw_query) {
		String processed_query = raw_query;
		// list of strings
		if (document_index.containsKey(processed_query)) {
			return document_index.get(processed_query);
		} else
			return null;
	}

}
