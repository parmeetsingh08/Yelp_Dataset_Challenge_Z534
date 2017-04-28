package finalProject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class TrainingAndTestingData {

	private static void fileReviews(String filename, String folder,
			HashMap<String, String> Reviews) {

		PrintWriter writer = null;
		for (Entry<String, String> entry : Reviews.entrySet()) {
			try {
				writer = new PrintWriter(new FileOutputStream(new File(
						"C:\\Users\\ShravanJagadish\\Desktop\\Search\\Final Project\\Output\\"
								+ folder + "\\" + filename + "_"
								+ entry.getKey() + ".txt"), true));
				writer.append(entry.getValue());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			writer.close();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			IndexReader reader = DirectoryReader
					.open(FSDirectory.open(Paths
							.get("C:/Users/ShravanJagadish/Desktop/Search/Final Project/Index")));
			IndexSearcher searcher = new IndexSearcher(reader);
			/**
			 * Get query terms from the query string 
			 * Change the range to 20140101 and 20141231 for extracting testing dataset
			 */			
			Query querys = NumericRangeQuery.newIntRange("DATE", 20110101,
					20131231, true, true);

			HashMap<String, String> Reviews = new HashMap<String, String>();
			
			/**
			 * Loop through all the data set based on dates and extract reviews
			 */			
			TopDocs docs = searcher.search(querys, 1);
			// System.out.println(docs.totalHits);
			TopDocs topDoc = searcher.search(querys, docs.totalHits);

			for (ScoreDoc scoreDoc : topDoc.scoreDocs) {
				Document doc = searcher.doc(scoreDoc.doc);
				System.out.println(doc.get("BID"));
				Reviews.put("one", Reviews.get("one") + doc.get("REV_TEXT1"));
				Reviews.put("two", Reviews.get("two") + doc.get("REV_TEXT2"));
				Reviews.put("three",
						Reviews.get("three") + doc.get("REV_TEXT3"));
				Reviews.put("four", Reviews.get("four") + doc.get("REV_TEXT4"));
				Reviews.put("five", Reviews.get("five") + doc.get("REV_TEXT5"));
			}
			/**
			 * Print the extracted data into text files
			 * Change to Test folder to save test data
			 */
			fileReviews("Bakeries", "Train", Reviews);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

}
