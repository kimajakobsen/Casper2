package dk.aau.cs.extbi.casper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;

public class SSBProcessor {
	private HashSet<String> custkeys = new HashSet<String>();
	private HashSet<String> datekeys = new HashSet<String>();
	private HashSet<String> partkeys = new HashSet<String>();
	private HashSet<String> suppkeys = new HashSet<String>();
	private ArrayList<String> URLs = new ArrayList<String>();
	private String path;
	private Dataset dataset;
	private boolean dataRead;
	private int numberOfFacts;
	private QueryExecution qexec;
	private int percentage;

	public SSBProcessor (String path) {
		this.path = path;
		dataset = TDBFactory.createDataset(this.path);
		deleteFullMaterialized(dataset);
	}
	
	public void readData(int percentage) {
		this.percentage = percentage;
		dataset.begin(ReadWrite.READ) ;
		
		
		this.numberOfFacts = convertToNumberOfFacts(percentage);
		System.out.println(numberOfFacts);
		
		
		//lineorderDefault();
		lineorderMod();
	
		//TODO it is a bit weird that it says customer and not custkey (maybe it amounts to the same thing)
		executeQuery(getCustomers(),"customer",custkeys);
		
		executeQuery(getPart(),"partkey",partkeys);
	
		executeQuery(getSupplier(),"suppkey",suppkeys);
		
		executeQuery(getDate(),"date",datekeys);
		
		dataset.commit();
		dataset.end();
		dataRead = true;
	}

	private void lineorderMod() {
		qexec = QueryExecutionFactory.create(getAllLineorders(), dataset);
		ResultSet lineorder = qexec.execSelect() ;
		
		
		int counter = 0;
		int lo = 0;
		int numberOfMatches = this.percentage/10;
		while (lineorder.hasNext())
		{
			QuerySolution bindings = lineorder.nextSolution();
			if (counter%10 < numberOfMatches) {
				//System.out.println("counter: "+counter+ " mod 10 = "+counter%10+" is less or equals than "+numberOfMatches);
				
				suppkeys.add(bindings.get("suppkey").toString());
				custkeys.add(bindings.get("customer").toString());
				datekeys.add(bindings.get("commitdate").toString());
				datekeys.add(bindings.get("orderdate").toString());
				partkeys.add(bindings.get("partkey").toString());
				URLs.add(bindings.get("loGraph").toString());
				lo++;
				//System.out.println(lo);
			}
			counter++;
		}
		System.out.println("Added "+ lo +" graphs from lineitem");
		
	}

	@SuppressWarnings("unused")
	private void lineorderDefault() {
		qexec = QueryExecutionFactory.create(getLineorder(numberOfFacts), dataset);
		ResultSet lineorder = qexec.execSelect() ;
		
		int lo = 0;
		while (lineorder.hasNext())
		{
			QuerySolution bindings = lineorder.nextSolution();
			suppkeys.add(bindings.get("suppkey").toString());
			custkeys.add(bindings.get("customer").toString());
			datekeys.add(bindings.get("commitdate").toString());
			datekeys.add(bindings.get("orderdate").toString());
			partkeys.add(bindings.get("partkey").toString());
			URLs.add(bindings.get("loGraph").toString());
			lo++;
		}
		System.out.println("Added "+ lo +" graphs from lineitem");
	}

	private void executeQuery(String query, String primarykey, HashSet<String> keys) {
		qexec = QueryExecutionFactory.create(query, dataset) ;
		ResultSet resultSet = qexec.execSelect() ;
		
		int d = 0;
		while (resultSet.hasNext())
		{
			QuerySolution bindings = resultSet.nextSolution();
			if (keys.contains(bindings.get(primarykey).toString())) {
				URLs.add(bindings.get("graph").toString());
				d++;
			}
		}
		System.out.println("Added "+ d +" graphs ("+primarykey+")");
	}
	
	private int convertToNumberOfFacts(int percentage) {
		qexec = QueryExecutionFactory.create(getNumberOfFacts(), dataset) ;
		ResultSet facts = qexec.execSelect() ;
		int numberOfFacts = 0;
		
		if (facts.hasNext()) {
			QuerySolution bindings = facts.nextSolution();
			numberOfFacts = Integer.valueOf(bindings.get("count").toString().split("\\^\\^")[0]);
		}
		return (numberOfFacts*percentage)/100 ;
	}

	private void deleteFullMaterialized(Dataset dataset) {
    	dataset.begin(ReadWrite.WRITE) ;
    	UpdateRequest request = UpdateFactory.create() ;
    	request.add("DROP GRAPH <http://example.com/fullMaterilized>") ;

    	// And perform the operations.
    	UpdateAction.execute(request, dataset) ;
    	dataset.commit();
	}
	
	private String getSupplier() {
    	String query =""+
    			"select ?suppkey ?graph {" +
    			"    GRAPH ?graph {" +
    			"        ?suppkey <http://example.com/supkey> ?supp" +
    			"    }" +
    			"}" ;
    	return query;
	}

	private String getDate() {
		String query =""+
    			"select ?date ?graph {" +
    			"    GRAPH ?graph {" +
    			"        ?date <http://example.com/date> ?supp" +
    			"    }" +
    			"}" ;
    	return query;
	}

	private String getPart() {
		String query =""+
    			"select ?partkey ?graph {" +
    			"    GRAPH ?graph {" +
    			"        ?partkey <http://example.com/partkey> ?supp" +
    			"    }" +
    			"}" ;
    	return query;
	}

	private String getCustomers() {
		String query =""+
    			"select ?customer ?graph {" +
    			"    GRAPH ?graph {" +
    			"        ?customer <http://example.com/custkey> ?supp" +
    			"    }" +
    			"}" ;
    	return query;
	}
	
	private String getNumberOfFacts() {
		String query = ""+
    		    "select (count(distinct ?s) as ?count) { " +
    		    "    Graph ?g {" +
    		    "        ?s  a <http://purl.org/linked-data/cube#Observation> ." +
    		    "    }" +
    		    "}" ;
    	return query;
	}

	private String getLineorder(int numberOfFacts) {
    	String query = ""+
    		    "select distinct ?a ?loGraph ?suppkey ?partkey ?customer ?orderdate ?commitdate { " +
    		    "    Graph ?metadata {" +
    		    "        ?a  <http://example.com/custkey> ?customer ;" +
    		    "            <http://example.com/partkey> ?partkey ;" +
    		    "            <http://example.com/suppkey> ?suppkey ;" +
    		    "            <http://example.com/orderdate> ?orderdate ;" +
    		    "            <http://example.com/commitdate> ?commitdate ." +
    		    "        {" +
    		    "            Select distinct ?a ?loGraph      " +
    		    "            { " +
    		    "                GRAPH ?loGraph {" +
    		    "                    ?a <http://example.com/revenue> ?c" +
    		    "                }" +
    		    "            }" +
    		    "            limit "+numberOfFacts+
    		    "        } " +
    		    "    }" +
    		    "}" ;
    	return query;
    }
	
	private String getAllLineorders() {
    	String query = ""+
    		    "select distinct ?a ?loGraph ?suppkey ?partkey ?customer ?orderdate ?commitdate { " +
    		    "    Graph ?metadata {" +
    		    "        ?a  <http://example.com/custkey> ?customer ;" +
    		    "            <http://example.com/partkey> ?partkey ;" +
    		    "            <http://example.com/suppkey> ?suppkey ;" +
    		    "            <http://example.com/orderdate> ?orderdate ;" +
    		    "            <http://example.com/commitdate> ?commitdate ." +
    		    "        {" +
    		    "            Select distinct ?a ?loGraph      " +
    		    "            { " +
    		    "                GRAPH ?loGraph {" +
    		    "                    ?a <http://example.com/revenue> ?c" +
    		    "                }" +
    		    "            }" +
    		    "        } " +
    		    "    }" +
    		    "}" ;
    	return query;
    }
	
	public  void writeToDisk() {
		if (dataRead == true) {
			File theDir = new File(path+"provenanceQuery/");
	    	if (!theDir.exists()) {
	    	    try{
	    	        theDir.mkdir();
	    	    } 
	    	    catch(SecurityException se){
	    	    	se.printStackTrace();
	    	    }        
	    	}
	    	
	    	PrintWriter writer;
			try {
				writer = new PrintWriter(path+"provenanceQuery/"+"query"+percentage+".txt", "UTF-8");
				for (String string : URLs) {
					writer.println(string);
				}
		    	writer.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("readData need to be invoked before writeToDisk can be executed.");
		}
		
    	
	}
}
