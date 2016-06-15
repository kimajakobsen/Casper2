package dk.aau.cs.extbi.casper;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class App 
{
	public static void main( String[] args )
    {
		// create the command line parser
		CommandLineParser parser = new DefaultParser();
	
		// create the Options
		Options options = new Options();
		options.addOption("h", "help", false, "Display this message." );
		options.addOption("s", "split", true, "comma sperated percentage values where the split will occure");
		options.addOption("p", "paths", true, "comma seperated paths to tdb datasets");
		
		try {
		    CommandLine line = parser.parse( options, args );
		    
		    if (line.hasOption( "help" )) {
		    	printHelp(null,options);
		    	System.exit(0);
			} 
		    
		    if (line.hasOption("split")) {
				Config.addPercentageValues(line.getOptionValue("split"));
			}
		    
		    if (line.hasOption("paths")) {
				Config.addPaths(line.getOptionValue("paths"));
			}
		   
		}
		catch( ParseException exp ) {
			printHelp(exp, options);
		} catch (Exception exp) {
			exp.printStackTrace();
		}
		
		for (String path : Config.getPaths()) {
			for (String percentage : Config.getPercentageValues()) {
				System.out.println("Path: "+ path + " percentage: " + percentage);
				SSBProcessor processor = new SSBProcessor(path);
				processor.readData(Integer.valueOf(percentage));
				processor.writeToDisk();
			}
		}
	}
			
	private static void printHelp(ParseException exp, Options options) {
		String header = "";
		HelpFormatter formatter = new HelpFormatter();
		if (exp != null) {
			header = "Unexpected exception:" + exp.getMessage();
		}
		formatter.printHelp("myapp", header, options, null, true);
    }
}
