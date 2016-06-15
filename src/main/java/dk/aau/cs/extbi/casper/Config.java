package dk.aau.cs.extbi.casper;

import java.util.ArrayList;

public class Config {

	static private ArrayList<String> paths = new ArrayList<String>();
	static private ArrayList<String> percentageValues = new ArrayList<String>();
	
	static public ArrayList<String> getPaths() {
		return paths;
	}
	
	static public void addPaths(String paths) {
		for (String path : paths.split(",")) {
			addPath(path);
		}
	}
	
	static public void addPath(String path) {
		paths.add(path);
	}
	
	static public ArrayList<String> getPercentageValues() {
		return percentageValues;
	}
	
	static public void addPercentageValues(String percentageValues) {
		for (String percentageValue : percentageValues.split(",")) {
			addPercentageValue(percentageValue);
		}
	}

	static public void addPercentageValue(String percentageValue) {
		percentageValues.add(percentageValue);
	}
}
