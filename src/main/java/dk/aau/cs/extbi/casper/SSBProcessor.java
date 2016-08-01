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
		
		qexec = QueryExecutionFactory.create(getCustomers(), dataset) ;
		ResultSet customer = qexec.execSelect() ;
		
		int c = 0;
		while (customer.hasNext())
		{
			QuerySolution bindings = customer.nextSolution();
			if (custkeys.contains(bindings.get("customer").toString())) {
				URLs.add(bindings.get("graph").toString());
				c++;
			}
		}
		System.out.println("Added "+ c +" graphs from customer");
		
		qexec = QueryExecutionFactory.create(getPart(), dataset) ;
		ResultSet part = qexec.execSelect() ;
		
		int p = 0;
		while (part.hasNext())
		{
			QuerySolution bindings = part.nextSolution();
			if (partkeys.contains(bindings.get("partkey").toString())) {
				URLs.add(bindings.get("graph").toString());
				p++;
			}
		}
		System.out.println("Added "+ p +" graphs from part");
		
		qexec = QueryExecutionFactory.create(getSupplier(), dataset) ;
		ResultSet supplier = qexec.execSelect() ;
		
		int s = 0;
		while (supplier.hasNext())
		{
			QuerySolution bindings = supplier.nextSolution();
			if (suppkeys.contains(bindings.get("suppkey").toString())) {
				URLs.add(bindings.get("graph").toString());
				s++;
			}
		}
		System.out.println("Added "+ s +" graphs from supplier");
		
		qexec = QueryExecutionFactory.create(getDate(), dataset) ;
		ResultSet date = qexec.execSelect() ;
		
		int d = 0;
		while (date.hasNext())
		{
			QuerySolution bindings = date.nextSolution();
			if (datekeys.contains(bindings.get("date").toString())) {
				URLs.add(bindings.get("graph").toString());
				d++;
			}
		}
		System.out.println("Added "+ d +" graphs from date");
		
		dataset.commit();
		dataset.end();
		dataRead = true;
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
