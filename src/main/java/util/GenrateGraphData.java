package util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class GenrateGraphData {

	public static void main(String[] args) throws IOException {
		final List<String> lines = Files.readAllLines(Paths.get("/Users/farshadbakhshandeganmoghaddam/Desktop/Generated_Features_Automatic.txt"));
		final List<String> finalResult = new  ArrayList<>();
		for(String line:lines) {
			if(line.startsWith("<-")) {
				
			}else {
				StringBuilder result = new StringBuilder();
				final String[] parts = line.split("->");
				for(int i=0;i<parts.length;i++) {
					if(i==0) {
						result.append("{source: \"");
						result.append("accident\",");
						result.append("target: \"");
						result.append(parts[i+1]);
						result.append("\", type: \"licensing\"}");
						result.append(",\n");
						
					}
					else if(i==parts.length-1) {
						//nothing
					}else {
						result.append("{source: \"");
						result.append(parts[i]).append("\",");
						result.append("target: \"");
						result.append(parts[i+1]);
						result.append("\", type: \"licensing\"}");
						result.append(",\n");
					}
				}
				finalResult.add(result.toString());
			}
		}
		
		
		finalResult.forEach(p -> System.out.println(p));

	}

}
