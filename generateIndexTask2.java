package finalProject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class generateIndexTask2 {
	private static File indexFile;		//Stores index file location
	private static File corpusFile;		//Stores corpus file location
	
	private void displayStats() throws IOException {
		/**
		 * This function displays statistics about corpus
		 */
		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths
				.get(indexFile.toString())));

		System.out.println("Total number of documents in the corpus:: "
				+ reader.maxDoc());
	}

	private void createIndex() throws IOException {
		/**
		 * This function creates lucene index
		 */

		Directory dir = FSDirectory.open(Paths.get(indexFile.toString()));
		Analyzer analyzer = new StandardAnalyzer();
		IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
		writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		IndexWriter writer = new IndexWriter(dir, writerConfig);

		File[] allFiles = corpusFile.listFiles(); // Identify files within
													// corpus path
		for (File file : allFiles) {

			// Pick file with extension - '.trec'
			if (file.toString().contains(".trec")) {
				String content = new String(Files.readAllBytes(Paths.get(file
						.toString())));

				String[] DOC = StringUtils.substringsBetween(content, "<DOC>",
						"</DOC>"); // pickup contents between each <DOC> tag
				// Loop through the DOC tags to fetch required tags for each
				// entry
				for (String doc : DOC) {
					String BID, CITY, CATEGORY;
					java.util.Date DATE = null;
					String REV_TEXT1, REV_TEXT2, REV_TEXT3, REV_TEXT4, REV_TEXT5, TIP_TEXT, RATING;
					
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
					
					BID = StringUtils.substringBetween(doc, "<BID>", "</BID>");
					CITY = StringUtils.substringBetween(doc, "<CITY>",
							"</CITY>");
					CATEGORY = StringUtils.substringBetween(doc, "<CAT>",
							"</CAT>");
					try {
						DATE = formatter.parse(StringUtils.substringBetween(doc, "<DATE>",
								"</DATE>"));
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					REV_TEXT1 = StringUtils.substringBetween(doc,
							"<REV_TEXT_1>", "</REV_TEXT_1>");
					REV_TEXT2 = StringUtils.substringBetween(doc,
							"<REV_TEXT_2>", "</REV_TEXT_2>");
					REV_TEXT3 = StringUtils.substringBetween(doc,
							"<REV_TEXT_3>", "</REV_TEXT_3>");
					REV_TEXT4 = StringUtils.substringBetween(doc,
							"<REV_TEXT_4>", "</REV_TEXT_4>");
					REV_TEXT5 = StringUtils.substringBetween(doc,
							"<REV_TEXT_5>", "</REV_TEXT_5>");
					TIP_TEXT = StringUtils.substringBetween(doc, "<TIP_TEXT>",
							"</TIP_TEXT>");
					RATING = StringUtils.substringBetween(doc,
							"<RATING_COUNT>", "</RATING_COUNT>");

					// Generate Index for one entry
					String dat = DateTools.dateToString(DATE, Resolution.DAY);
					Integer d = Integer.parseInt(dat);
					//System.out.println(d);
					
					Document lucene = new Document();
					lucene.add(new TextField("BID", BID, Field.Store.YES));
					lucene.add(new TextField("CITY", CITY, Field.Store.YES));
					lucene.add(new TextField("CATEGORY", CATEGORY,
							Field.Store.YES));
					lucene.add(new IntField("DATE", d, Field.Store.YES));
					lucene.add(new TextField("REV_TEXT1", REV_TEXT1,
							Field.Store.YES));
					lucene.add(new TextField("REV_TEXT2", REV_TEXT2,
							Field.Store.YES));
					lucene.add(new TextField("REV_TEXT3", REV_TEXT3,
							Field.Store.YES));
					lucene.add(new TextField("REV_TEXT4", REV_TEXT4,
							Field.Store.YES));
					lucene.add(new TextField("REV_TEXT5", REV_TEXT5,
							Field.Store.YES));
					lucene.add(new TextField("TIP_TEXT", TIP_TEXT,
							Field.Store.YES));
					lucene.add(new TextField("RATING", RATING, Field.Store.YES));
					writer.addDocument(lucene);
				}

			}
		}
		writer.forceMerge(1);
		writer.commit();
		writer.close();
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		@SuppressWarnings("resource")
		Scanner sc = new Scanner(System.in);
		generateIndexTask2 generator = new generateIndexTask2();

		// Get corpus file path from user
		System.out.println("Please provide corpus directory location:: ");
		String userCorpusDir = sc.nextLine();
		corpusFile = new File(userCorpusDir);

		// Get directory path to store index files from user
		System.out
				.println("Please provide directory location to store index files:: ");
		String userIndexDir = sc.nextLine();
		indexFile = new File(userIndexDir);

		// For every file in directory create index and print vocabulary
		if (corpusFile.exists() && indexFile.exists()) {
			if (corpusFile.isDirectory() && indexFile.isDirectory()) {
				generator.createIndex(); // create index
				generator.displayStats(); // display document stats
			}
		} else {
			System.out.println("Invalid input! Try again");
		}
	}

}
