import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class taskOne {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException{

		//index the training review text
		try {
        //to change to respective dir and run the python code            
        ProcessBuilder builder = new ProcessBuilder(
                "cmd.exe", "/c", "cd \"C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\" && python new_task1.py");

        Process pr = builder.start();

        //to print console output
        BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

        String line=null;

        while((line=input.readLine()) != null) {
            System.out.println(line);
        }

        int exitVal = pr.waitFor();
        System.out.println("Exited with error code "+exitVal);

    } catch(Exception e) {
        System.out.println(e.toString());
        e.printStackTrace();
    }
		//run mallet on the training data
		try {
        //to change to respective dir and run the python code            
        ProcessBuilder builder = new ProcessBuilder(
                "cmd.exe", "/c", "cd \"C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\" && python get_topics.py");

        Process pr = builder.start();

        //to print console output
        BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

        String line=null;

        while((line=input.readLine()) != null) {
            System.out.println(line);
        }

        int exitVal = pr.waitFor();
        System.out.println("Exited with error code "+exitVal);

    } catch(Exception e) {
        System.out.println(e.toString());
        e.printStackTrace();
    }

	//index the rest of the test review data
		try {
        //to change to respective dir and run the python code            
        ProcessBuilder builder = new ProcessBuilder(
                "cmd.exe", "/c", "cd \"C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\" && python task1.py");

        Process pr = builder.start();

        //to print console output
        BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));

        String line=null;

        while((line=input.readLine()) != null) {
            System.out.println(line);
        }

        int exitVal = pr.waitFor();
        System.out.println("Exited with error code "+exitVal);

    } catch(Exception e) {
        System.out.println(e.toString());
        e.printStackTrace();
    }


		JSONParser parser = new JSONParser();
		try {
			String line1;
			FileReader business_file = new FileReader("C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\yelp_academic_dataset_business.json");
			FileReader review_file = new FileReader("C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\yelp_academic_dataset_review.json");
			BufferedReader br1  = new BufferedReader (business_file);
			BufferedReader br2  = new BufferedReader (review_file);
			HashMap<String, Integer> cat_list = new HashMap<String, Integer>();
			ArrayList<String> new_cat_list = new ArrayList<String>();
			HashMap<String, List<String>> categories = new HashMap<String, List<String>>();

			int count=0;
			while(null != (line1 = br1.readLine())) //Get all categories present & the number of businesses in that category
			{
				JSONObject jsonObject = (JSONObject) parser.parse(line1);
				String bid = (String) jsonObject.get("business_id");
				List<String> cats = (List) jsonObject.get("categories");
				categories.put(bid, cats);
				for (String cat : cats)
				{
					if(!cat_list.containsKey(cat))
					{
						cat_list.put(cat,count++);		//assign a value to each category
						new_cat_list.add(cat);
					}
				}
			}



			
//Index topic_keys files 

			System.out.println("Indexing is Starting");
			for(String category : new_cat_list)
			{

				String filePath="C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\topic_keys";

				String indexFilename = "\\_"+cat_list.get(category);
				String indexPath = "C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\task1_index"+indexFilename;
				final File docDir= new File(filePath);
				Directory dir=FSDirectory.open(Paths.get(indexPath));
				Analyzer analyzer= new StandardAnalyzer();
				IndexWriterConfig iwc= new IndexWriterConfig(analyzer);
				iwc.setOpenMode(OpenMode.CREATE);
				IndexWriter writer=new IndexWriter(dir,iwc);
				File[] allFiles=docDir.listFiles();

				int i=1;


				for(File file: allFiles)
				{
					//get filename = encrypted business id 
					String absoluteFileName=file.getName();
					String actualFileName=absoluteFileName.substring(0,absoluteFileName.indexOf("_topics.txt"));
					List<String> bus_cats = categories.get(actualFileName);	//get the categories for the business
					if(bus_cats!=null && bus_cats.contains(category))
					{


						Document lDoc=new Document();
						Reader reader=new FileReader(file);
						lDoc.add(new StringField("DOCNO",actualFileName,Field.Store.YES));
						lDoc.add(new Field("TEXT",reader));

						writer.addDocument(lDoc);
						i++;


					}


				}

				writer.forceMerge(1);
				writer.commit();
				writer.close();
			}
			System.out.println("Indexing is over");

			// code to index the 60% of the review text 

						System.out.println("Indexing of rev_text is Starting");

						String filePath="C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\task1_output_test";

						

						final File docDir= new File(filePath);
						File[] allFiles=docDir.listFiles();

						int i=1;

						int ten_percent=0;

						String indexPath = "C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\task1_rev_index_3";
						int index_count=0;
						for(File file: allFiles)
						{

							//if(index_count<10){
							String absoluteFileName=file.getName();
							String actualFileName=absoluteFileName.substring(0,absoluteFileName.indexOf(".txt"));
							Directory dir=FSDirectory.open(Paths.get(indexPath+"\\_"+actualFileName));
							Analyzer analyzer= new StandardAnalyzer();
							IndexWriterConfig iwc= new IndexWriterConfig(analyzer);
							iwc.setOpenMode(OpenMode.CREATE);
							IndexWriter writer=new IndexWriter(dir,iwc);
							Document lDoc=new Document();
							Reader reader=new FileReader(file);
							lDoc.add(new StringField("DOCNO",actualFileName,Field.Store.YES));
							lDoc.add(new Field("TEXT",reader));

							writer.addDocument(lDoc);
							i++;

							writer.forceMerge(1);
							writer.commit();
							writer.close();
							//}
							System.out.println(index_count++);
						}


			 //end of indexing review text

			//get the most frequent n topic keys 
			for(int a=0;a<1;a++)
			{
				//calc all unique keys in one cat and add the top 5 unique words to each category query list
				int new_val=15;
				new_val = new_val *(a+1);
				HashMap<String,ArrayList<String>> query_map = new HashMap<String,ArrayList<String>>();
				int inside=0;
				for( String cat : new_cat_list)
				{
					HashMap <String,Long> term_freq = new HashMap<String,Long>();
					ArrayList<String> keywords = new ArrayList<String>();

					ArrayList<String> unique_terms= new ArrayList<String>();
					//String cat = new_cat_list.get(726);
					//System.out.println(cat);
					int cat_number = cat_list.get(cat);
					String indexPath1="C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\task1_index\\_"+cat_number;
					Directory dir=FSDirectory.open(Paths.get(indexPath1));
					IndexReader ir = DirectoryReader.open(dir);
					//System.out.println(inside++);
					Terms vocabulary = MultiFields.getTerms(ir, "TEXT");
					String new_cat = cat;
					if(vocabulary == null)
					{
						if(cat.contains("&") || cat.contains(",") || cat.contains("/"))
						{
							new_cat =new_cat.replaceAll("&", " ");
							new_cat =new_cat.replaceAll(",", " ");
							new_cat =new_cat.replaceAll("/", " ");
						}
						String split_words[] = new_cat.split("\\s+");
						for(String word : split_words)
							keywords.add(word);

					}
					else
					{
						TermsEnum iterator = vocabulary.iterator();
						BytesRef byteRef = null;
						while((byteRef = iterator.next()) != null) 
						{
							String term1 = byteRef.utf8ToString();				
							if(!unique_terms.contains(term1))
								unique_terms.add(term1);
						}
						for (String terms : unique_terms)
							term_freq.put(terms, ir.totalTermFreq(new Term("TEXT",terms)));
						List<Entry<String, Long>> sorted_terms = entriesSortedByValues(term_freq);
						if(cat.contains("&") || cat.contains(",") || cat.contains("/"))
						{
							new_cat = new_cat.replaceAll("&", " ");
							new_cat = new_cat.replaceAll(",", " ");
							new_cat = new_cat.replaceAll("/", " ");
						}
						String split_words[] = new_cat.split("\\s+");
						for(String word : split_words)
							keywords.add(word);
						int i1=0;
						while(i1<new_val)
						{

							Entry e = sorted_terms.get(i1);
							String key = (String) e.getKey();
							//if(cat_number==724)
							//System.out.println(key);
							if(!cat.contains(key) && !key.equals("it\'s") && !key.equals("don\'t") && !key.equals("didn\'t") && !key.equals("i\'m") && !key.equals("i\'ve") && !key.equals("doesn\'t") && !key.equals("that\'s") && !key.equals("needn\'t"))
								keywords.add(key);
							i1++;
						}

					}
					query_map.put(cat,keywords);
				}
				ArrayList<String> values = new ArrayList<String>();
				int cat_count=0;
				String[] query_strings = new String[query_map.size()];
				for(Entry e : query_map.entrySet())
				{
					values = (ArrayList<String>) e.getValue();

					for (String value : values)
					{
						query_strings[cat_count]+=value+" ";
					}
					cat_count++;
				}




			




				//get relevance scores for each category for all businesses

				
				String filePath1="C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\task1_output_test";
				final File docDir1= new File(filePath1);
				File[] allFiles1=docDir1.listFiles();
				int i1=1;
				int ten_percent1=0;
				int index_count1=0;
				PrintWriter output_file = new PrintWriter("C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\out_"+(a+1)+".txt","UTF-8");
				for(File file: allFiles1)
				{
					/*if(index_count1<2000)
					{
						index_count1++;
						continue;
					}*/
						HashMap<String,ArrayList<String>> assigned_category = new HashMap<String,ArrayList<String>>();
						String absoluteFileName=file.getName();
						String actualFileName=absoluteFileName.substring(0,absoluteFileName.indexOf(".txt"));

						for(Entry list : query_map.entrySet())
						{
							String cat_name = new String();

							cat_name = (String) list.getKey();
							String indexFilename = "\\_"+actualFileName;

							//}

							HashMap<String,Double> s_list=getRelevanceScore((ArrayList<String>) list.getValue(),indexFilename);//query_strings[i]);

							ArrayList<String> c_list = new ArrayList<String>();
							for (Map.Entry<String, Double> entity: s_list.entrySet())	        	
							{

								if(assigned_category.containsKey(entity.getKey()))
								{
									c_list = assigned_category.get(entity.getKey());

								}
								if(!c_list.contains(cat_name) && (entity.getValue()>0.02) && c_list.size()<30)
								{
									c_list.add(cat_name);
									assigned_category.put(entity.getKey(),c_list);
								}
							}


						}
						System.out.println(index_count1++);
						for(Entry e1 : assigned_category.entrySet())
						{
							if(e1 == null)
								System.out.println("hello1");
							output_file.println(e1.getKey()+"\t"+e1.getValue());

						}
				}	
				output_file.close();




			}


			br1.close();
			br2.close();


		} catch (Exception e) {
			e.printStackTrace();
		}
	}




	private static ArrayList<String> getHighestScores(HashMap<String, Double> s_list) {
		// TODO Auto-generated method stub
		
		ArrayList<String> highest_keys = new ArrayList<String>();
		for(int i=0;i<5;i++)
		{
			Entry<String,Double> maxEntry = null;
			for(Entry<String,Double> entry : s_list.entrySet())
			{
				if (maxEntry == null || entry.getValue() > maxEntry.getValue()) {
			        maxEntry = entry;
			    }
			}
			highest_keys.add(maxEntry.getKey());
			s_list.remove(maxEntry.getKey());
		}
		return highest_keys;
	}




	static <K,V extends Comparable<? super V>> 
	List<Entry<K, V>> entriesSortedByValues(Map<K,V> map) {

		List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());

		Collections.sort(sortedEntries, 
				new Comparator<Entry<K,V>>() {
			@Override
			public int compare(Entry<K,V> e1, Entry<K,V> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		}
				);

		return sortedEntries;
	}

	public static HashMap<String,Double> getRelevanceScore(ArrayList<String> query_term_list, String filename) throws IOException, ParseException
	{
		int term_count=0,no_terms=0,df[],N=0;
		String []terms;
		double F[];

		HashMap<String,Double> scores_list = new HashMap<String,Double>();

		IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get("C:\\Users\\Susmitha\\Documents\\Fall 15\\Search\\final project\\yelp_dataset_challenge_academic_dataset\\task1_rev_index_3"+filename)));
		IndexSearcher searcher = new IndexSearcher(reader);
		/**
		 * Get query terms from the query string
		 */
		String queryString = new String();

		for (String value : query_term_list)
		{
			queryString+=value+" ";
		}

		queryString=queryString.trim();
		//String queryString = "New York";

		// Get the preprocessed query terms
		Analyzer analyzer = new StandardAnalyzer();
		QueryParser parser = new QueryParser("TEXT", analyzer);
		Query query = parser.parse(queryString);
		Set<Term> queryTerms = new LinkedHashSet<Term>();
		searcher.createNormalizedWeight(query,false).extractTerms(queryTerms);
		no_terms=queryTerms.size();
		terms=new String[no_terms];
		df=new int[no_terms];
		//System.out.println("Terms in the query: ");
		for (Term t : queryTerms) {

			terms[term_count]=t.text();
			df[term_count]=reader.docFreq(new Term("TEXT",terms[term_count]));		/* calculate df values for each term */
term_count++;

		}
		
		/* Get total number of document in Corpus */

		N=reader.maxDoc();

		F = new double[N];
		java.util.Arrays.fill(F, 0.0);

		/**
		 * Get document length and term frequency
		 */

		// Use DefaultSimilarity.decodeNormValue(…) to decode normalized
		// document length
		DefaultSimilarity dSimi = new DefaultSimilarity();
		// Get the segments of the index
		List<LeafReaderContext> leafContexts = reader.getContext().reader().leaves();
		// Processing each segment
		int count=0;

		double docLeng[]=new double[N];
		int d_count=0;
		//System.out.println(leafContexts.size());
		for (int i = 0; i < leafContexts.size(); i++) {
			LeafReaderContext leafContext = leafContexts.get(i);
			int startDocNo = leafContext.docBase;
			//System.out.println(startDocNo);
			int numberOfDoc = leafContext.reader().maxDoc();

			for (int docId = 0; docId < numberOfDoc; docId++) {
				// Get normalized length (1/sqrt(numOfTokens)) of the document
				float normDocLeng = dSimi.decodeNormValue(leafContext.reader().getNormValues("TEXT").get(docId));
				// Get length of the document
				docLeng[docId] = 1 / (normDocLeng * normDocLeng);
			}



			// Get frequency of the term "police" from its postings
			int t_count=0;
			for ( String t : terms){
				PostingsEnum de = MultiFields.getTermDocsEnum(leafContext.reader(),"TEXT", new BytesRef(t));
				int doc;
				if (de != null) {
					int n_docs =0;
					while ((doc = de.nextDoc()) != PostingsEnum.NO_MORE_DOCS)
					{

						F[de.docID()]+=(de.freq()/docLeng[de.docID()])*Math.log((1+(N/df[t_count])));
						scores_list.put(searcher.doc(de.docID()).get("DOCNO"), F[de.docID()]);
						n_docs++;
					}
				}

				t_count++;

			}			

		}
		return scores_list;
	}


	private static Map<String, Double> sort_map(HashMap<String, Double> scores_list) {
		// Convert Map to List
		List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(scores_list.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		// Convert sorted map back to a Map
		Map<String, Double> sorted_map = new LinkedHashMap<String, Double>();
		for (Iterator<Map.Entry<String, Double>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Double> entry = it.next();
			sorted_map.put(entry.getKey(), entry.getValue());
		}
		int count=0;
		return sorted_map;

	}



}