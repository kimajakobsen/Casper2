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
	private HashSet<String> lokeys = new HashSet<String>();
	private HashSet<String> dimensions = new HashSet<String>();
	private HashSet<String> crap = new HashSet<String>();
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
	}
	
	public void readData(int percentage) {
		this.percentage = percentage;
		dataset.begin(ReadWrite.READ) ;
		
		this.numberOfFacts = convertToNumberOfFacts(percentage);
		System.out.println(numberOfFacts);
		lineorderMod();
		System.out.println("lineorder keys: "+lokeys.size());
	
		for (String string : lokeys) {
			executeQuery(getAttributes(string),dimensions);
		}
		System.out.println("dimensions "+ dimensions.size());
		
		for (String string : dimensions) {
			executeQuery(getAttributes(string),crap);
		}
		System.out.println("URLs: " + URLs.size());
		
		dataset.commit();
		dataset.end();
		dataRead = true;
	}

	private String getAttributes(String string) {
		String query = ""+
		" select ?g ?o " +
				" WHERE { " +
				"  GRAPH ?g { " +
				"<"+string+"> " + "   ?p ?o  " +
				"  } " +
				" } ";

		return query;
	}

	private void lineorderMod() {
		qexec = QueryExecutionFactory.create(getAllLineorders(), dataset);
		ResultSet lineorder = qexec.execSelect() ;
		
		
		int counter = 0;
		int numberOfMatches = this.percentage/10;
		while (lineorder.hasNext())
		{
			QuerySolution bindings = lineorder.nextSolution();
			if (counter%10 < numberOfMatches) {
				//System.out.println("counter: "+counter+ " mod 10 = "+counter%10+" is less or equals than "+numberOfMatches);
				lokeys.add(bindings.get("li").toString());
			}
			counter++;
		}
	}


	private void executeQuery(String query, HashSet<String> keys) {
		qexec = QueryExecutionFactory.create(query, dataset) ;
		ResultSet resultSet = qexec.execSelect() ;
		
		while (resultSet.hasNext())
		{
			QuerySolution bindings = resultSet.nextSolution();
			URLs.add(bindings.get("g").toString());
			String object = bindings.get("o").toString();
			if (isURL(object)) {
				keys.add(bindings.get("o").toString());
			}
		}
	}
	
	private boolean isURL(String object) {
		if (object.contains(".com")) {
			return true;
		}
		return false;
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
	
	
	private String getNumberOfFacts() {
		String query = ""+
    		    "select (count(distinct ?s) as ?count) { " +
    		    "    Graph ?g {" +
    		    "        ?s  a <http://purl.org/linked-data/cube#Observation> ." +
    		    "    }" +
    		    "}" ;
    	return query;
	}

	
	
	private String getAllLineorders() {
    	String query = ""+
    			"SELECT ?li  " +
    			"WHERE { " +
    			"	GRAPH ?g1 { " +
    			"		?li a <http://purl.org/linked-data/cube#Observation> " +
    			"	} " +
    			"} " +
    			"GROUP BY ?li " ;
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
