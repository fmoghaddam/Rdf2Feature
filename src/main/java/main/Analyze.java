package main;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.sparql.resultset.TSVOutput;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import util.Config;

public class Analyze {

	private static final Logger LOG = Logger.getLogger(Analyze.class.getCanonicalName());

	private static final String INPUT_DATA = Config.getString("INPUT_DATA", ";");
	private static final String OUTPUT_FILE = Config.getString("OUTPUT_FILE", ";");
	private static final String OUTPUT_FILE_DELIMITER = Config.getString("OUTPUT_FILE_DELIMITER", ";");
	private static final float SAMPLING_PERCENTAGE = Config.getFloat("SAMPLING_PERCENTAGE", 1f);
	private static final int SAMPLING_NUMBER = Config.getInt("SAMPLING_NUNBER", 5);
	private static final int WALK_DEPTH = Config.getInt("WALK_DEPTH", 5);
	private static final boolean SAMPLING_BY_PERCENTAGE = Config.getBoolean("SAMPLING_BY_PERCENTAGE", false);
	private static final SearchDirection SEARCH_DIRECTION = SearchDirection
			.resolve(Config.getString("SERACH_DIRECTION", SearchDirection.OUT_GOING.name()));

	private static final String UNIQUE_STRING_TOKEN = "2d7428a6_b58c_4008_8575_f05549f16316";
	private static final int NUMBER_OF_THREADS = Config.getInt("NUMNER_OF_THREADS", 1);
	private static final ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

	private static final Set<String> uniqueFeatures = new HashSet<>();
	private static final Model model = ModelFactory.createDefaultModel();

	private static final String SEED_GENERATION_SPARQL = Config.getString("SEED_GENERATION_SPARQL", "");
	private static final String SEED_GENERATION_MAIN_VARIBALE = Config.getString("SEED_GENERATION_MAIN_VARIABLE", ";");
	private static Set<Node> seeds = new HashSet<>();

	public static void main(String[] args) {
		printConfigs();
		readDataAndLoadModel();
		LOG.info("-----------------------------------------------");
		seeds = getSeeds(SEED_GENERATION_SPARQL, SEED_GENERATION_MAIN_VARIBALE);
		// .stream().filter(p ->
		// p.toString().contains("201800002448")).collect(Collectors.toSet());
		LOG.info(seeds.size() + " seed has been selected.");
		LOG.info("-----------------------------------------------");

		if (SAMPLING_BY_PERCENTAGE) {
			runSearchWithPercentage(seeds);
		} else {
			runSearchWithNumbers(seeds);
		}
		LOG.info("-----------------------------------------------");
		LOG.info("Number of UniqueFeatures " + uniqueFeatures.size());
		uniqueFeatures.forEach(p -> LOG.info(p));
		LOG.info("-----------------------------------------------");
		final Query query = generateSparqlQuary();
		LOG.info("-----------------------------------------------");
		executeSparqlAndWriteOutput(query, model);

	}

	private static ResultSet executeSparql(Query query, Model model2) {
		QueryExecution qexec = QueryExecutionFactory.create(query, model2);
		return qexec.execSelect();
	}

	private static void executeSparqlAndWriteOutput(Query query, Model model) {
		try {
			LOG.info("Executing sparql query  ....");
			long now = System.currentTimeMillis();
			ResultSet results = executeSparql(query, model);
			LOG.info("Executing sparql query is done.");
			LOG.info("Executing Sparql took: "
					+ TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now) + " second");
			LOG.info("-----------------------------------------------");
			TSVOutput csv = new TSVOutput();
			now = System.currentTimeMillis();
			LOG.info("Saving Sparql result ....");
			csv.format(new FileOutputStream("outout.csv"), results);
			LOG.info("Saving Sparql result is done.");
			LOG.info("Saving Sparql result to output file took: "
					+ TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now) + " second");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Query generateSparqlQuary() {
		LOG.info("Genering sparql quey ...");
		long now = System.currentTimeMillis();
		final LinkedList<List<Triplet<String, String, String>>> variables = new LinkedList<>();
		variables.add(Arrays.asList(
				new Triplet<String, String, String>("?accident", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
						"<http://www.engie.fr/ontologies/accidentontology/RoadAccident>")));

		final ParameterizedSparqlString queryStr = new ParameterizedSparqlString();
		final LinkedHashSet<String> onlyVariableNames = new LinkedHashSet<>();
		onlyVariableNames.add(SEED_GENERATION_MAIN_VARIBALE);

		for (String a : uniqueFeatures) {
			if (!a.startsWith("<-")) {
				final List<Triplet<String, String, String>> localVaribales = new LinkedList<>();
				final String[] pathes = a.split("->");
				String var = "";
				for (int i = 1; i < pathes.length; i++) {
					String path = "<" + pathes[i] + ">";
					final String[] properties = pathes[i].split("/");
					String variableName;
					if (i == pathes.length - 1) {
						variableName = ("?" + UNIQUE_STRING_TOKEN + properties[properties.length - 1]).replace("#", "_")
								.replace("-", "_");
						variableName = ("?" + UNIQUE_STRING_TOKEN + generateVaribaleNameFromFullPath(pathes, i))
								.replace("#", "_").replace("-", "_");
					} else {
						variableName = ("?" + properties[properties.length - 1]).replace("#", "_").replace("-", "_");
						variableName = ("?" + generateVaribaleNameFromFullPath(pathes, i)).replace("#", "_")
								.replace("-", "_");
					}

					if (var.isEmpty()) {
						var = variableName;
						localVaribales.add(
								new Triplet<String, String, String>(SEED_GENERATION_MAIN_VARIBALE, path, variableName));
					} else {
						localVaribales.add(new Triplet<String, String, String>(var, path, variableName));
						var = variableName;
					}
				}
				variables.add(localVaribales);
				localVaribales.forEach(p -> {
					if (p.getValue0().contains("?" + UNIQUE_STRING_TOKEN)) {
						onlyVariableNames.add(p.getValue0().replace("?" + UNIQUE_STRING_TOKEN, "?"));
					}
					if (p.getValue2().contains("?" + UNIQUE_STRING_TOKEN)) {
						onlyVariableNames.add(p.getValue2().replace("?" + UNIQUE_STRING_TOKEN, "?"));
					}

				});
			} else {
				a = a.substring(2);
				final List<Triplet<String, String, String>> localVaribales = new LinkedList<>();
				final String[] pathes = a.split("->");
				String var = "";
				for (int i = 0; i < pathes.length; i++) {
					String path = "<" + pathes[i] + ">";
					final String[] properties = pathes[i].split("/");
					String variableName;
					if (i == pathes.length - 1) {
						variableName = ("?" + UNIQUE_STRING_TOKEN + properties[properties.length - 1]).replace("#", "_")
								.replace("-", "_");
						variableName = ("?" + UNIQUE_STRING_TOKEN + generateVaribaleNameFromFullPath(pathes, i))
								.replace("#", "_").replace("-", "_");
					} else {
						variableName = ("?" + properties[properties.length - 1]).replace("#", "_").replace("-", "_");
						variableName = ("?" + generateVaribaleNameFromFullPath(pathes, i)).replace("#", "_")
								.replace("-", "_");

					}

					if (var.isEmpty()) {
						var = variableName;
						localVaribales.add(
								new Triplet<String, String, String>(variableName, path, SEED_GENERATION_MAIN_VARIBALE));
					} else {
						localVaribales.add(new Triplet<String, String, String>(var, path, variableName));
						var = variableName;
					}
				}
				variables.add(localVaribales);
				localVaribales.forEach(p -> {
					if (p.getValue0().contains("?" + UNIQUE_STRING_TOKEN)) {
						onlyVariableNames.add(p.getValue0().replace("?" + UNIQUE_STRING_TOKEN, "?"));
					}
					if (p.getValue2().contains("?" + UNIQUE_STRING_TOKEN)) {
						onlyVariableNames.add(p.getValue2().replace("?" + UNIQUE_STRING_TOKEN, "?"));
					}

				});
			}

		}

		boolean firstLine = true;
		queryStr.append("SELECT ");
		for (String s : onlyVariableNames) {
			queryStr.append(s + " ");
		}
		queryStr.append(" WHERE {");
		for (List<Triplet<String, String, String>> var : variables) {
			if (!firstLine) {
				queryStr.append(" OPTIONAL {");
			}
			for (Triplet<String, String, String> t : var) {
				queryStr.append(t.getValue0().replace("?" + UNIQUE_STRING_TOKEN, "?"));
				queryStr.append(" ");
				queryStr.append(t.getValue1().replace("?" + UNIQUE_STRING_TOKEN, "?"));
				queryStr.append(" ");
				queryStr.append(t.getValue2().replace("?" + UNIQUE_STRING_TOKEN, "?"));
				queryStr.append(" . ");
			}
			if (!firstLine) {
				queryStr.append(" } ");
			}
			firstLine = false;
		}
		queryStr.append("}");
		LOG.info(queryStr.toString());
		LOG.info("Generating sparql query is done.");
		LOG.info("Generating sparql query took: "
				+ TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now) + " second");
		return queryStr.asQuery();
	}

	private static String generateVaribaleNameFromFullPath(String[] pathes, int threshold) {
		final StringBuilder result = new StringBuilder();
		for (int i = 0; i <= threshold; i++) {
			String path = pathes[i];
			final String[] split = path.split("/");
			if (i != threshold) {
				if (split[split.length - 1].trim().isEmpty()) {
					continue;
				}
				result.append(split[split.length - 1]).append("_");
			} else {
				result.append(split[split.length - 1]);
			}
		}
		return result.toString();
	}

	private static void runSearchWithPercentage(final Set<Node> seeds) {
		LOG.info("Genering features by doing graph serach ...");
		long now = System.currentTimeMillis();
		try {
			for (Node node : seeds) {
				if (Math.random() <= SAMPLING_PERCENTAGE) {
					performSearch(node);
				}
			}

			executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		LOG.info("Genering features by doing graph search is done");
		LOG.info("Genering features by doing graph search took: "
				+ TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now) + " second");
	}

	private static void runSearchWithNumbers(final Set<Node> seeds) {
		LOG.info("Genering features by doing graph serach ...");
		long now = System.currentTimeMillis();
		try {
			int count = 0;
			final List<Node> seedsList = new ArrayList<>(seeds);
			Collections.shuffle(seedsList);
			for (Node node : seeds) {
				executor.submit(performSearch(node));
				if (++count >= SAMPLING_NUMBER) {
					break;
				}
			}
			executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		LOG.info("Genering features by doing graph search is done");
		LOG.info("Genering features by doing graph search took: "
				+ TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now) + " second");
	}

	private static Runnable performSearch(Node node) {
		final Runnable runnable = () -> {
			final Stack<Pair<Node, StringBuilder>> stack = new Stack<>();
			stack.add(new Pair<Node, StringBuilder>(node, new StringBuilder()));
			final List<Pair<String, Object>> propertyAndValue = new ArrayList<Pair<String, Object>>();
			while (!stack.isEmpty()) {
				final Pair<Node, StringBuilder> pop = stack.pop();
				final Node popNode = pop.getValue0();

				if (pop.getValue1().toString().split("->").length > WALK_DEPTH) {
					continue;
				}

				if (popNode.isLiteral()) {
					propertyAndValue.add(new Pair<String, Object>(pop.getValue1().toString(), popNode));
					uniqueFeatures.add(pop.getValue1().toString());
				}

				performSerach2(stack, pop);
			}
			LOG.info("Node is done: " + node.getURI());
		};
		return runnable;
	}

	private static void performSerach2(Stack<Pair<Node, StringBuilder>> stack, Pair<Node, StringBuilder> pop) {
		final Node node = pop.getValue0();
		final List<Triple> outgoingNeighbors = new ArrayList<>();
		final List<Triple> incomingNeighbors = new ArrayList<>();
		final String nodeString = node.toString();

		if (SEARCH_DIRECTION == SearchDirection.BOTH) {
			if (node.isBlank()) {
				final StmtIterator it = model.listStatements();
				Triple triple = null;
				while (it.hasNext()) {
					triple = it.next().asTriple();
					final String subjectString = triple.getSubject().toString();
					if (subjectString.equals(nodeString)) {
						outgoingNeighbors.add(triple);
					}
				}

			} else {
				ParameterizedSparqlString queryStr = new ParameterizedSparqlString();
				queryStr.append("SELECT ?s ?p ?o WHERE ");
				queryStr.append("{");
				queryStr.append("?s ?p ?o .");
				queryStr.append("FILTER (?s = ");
				queryStr.appendNode(node);
				queryStr.append(").");
				queryStr.append("} ");

				Query query = queryStr.asQuery();
				QueryExecution qexec = QueryExecutionFactory.create(query, model);
				try {
					ResultSet results = qexec.execSelect();
					while (results.hasNext()) {
						QuerySolution soln = results.nextSolution();
						final RDFNode subjectNode = soln.get("s");
						final RDFNode propertyNode = soln.get("p");
						final RDFNode objectNode = soln.get("o");
						Triple t = new Triple(subjectNode.asNode(), propertyNode.asNode(), objectNode.asNode());
						outgoingNeighbors.add(t);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				///////////////////////////////////

				if (seeds.contains(node)) {
					queryStr = new ParameterizedSparqlString();
					queryStr.append("SELECT ?s ?p ?o WHERE ");
					queryStr.append("{");
					queryStr.append("?s ?p ?o .");
					queryStr.append("FILTER (?o = ");
					queryStr.appendNode(node);
					queryStr.append(").");
					queryStr.append("} ");

					query = queryStr.asQuery();
					qexec = QueryExecutionFactory.create(query, model);
					try {
						ResultSet results = qexec.execSelect();
						while (results.hasNext()) {
							QuerySolution soln = results.nextSolution();
							final RDFNode subjectNode = soln.get("s");
							final RDFNode propertyNode = soln.get("p");
							final RDFNode objectNode = soln.get("o");
							Triple t = new Triple(subjectNode.asNode(), propertyNode.asNode(), objectNode.asNode());
							incomingNeighbors.add(t);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		} else if (SEARCH_DIRECTION == SearchDirection.OUT_GOING) {
			if (node.isBlank()) {
				final StmtIterator it = model.listStatements();
				Triple triple = null;
				while (it.hasNext()) {
					triple = it.next().asTriple();
					final String subjectString = triple.getSubject().toString();
					if (subjectString.equals(nodeString)) {
						outgoingNeighbors.add(triple);
					}
				}

			} else {
				ParameterizedSparqlString queryStr = new ParameterizedSparqlString();
				queryStr.append("SELECT ?s ?p ?o WHERE ");
				queryStr.append("{");
				queryStr.append("?s ?p ?o .");
				queryStr.append("FILTER (?s = ");
				queryStr.appendNode(node);
				queryStr.append(").");
				queryStr.append("} ");

				Query query = queryStr.asQuery();
				QueryExecution qexec = QueryExecutionFactory.create(query, model);
				try {
					ResultSet results = qexec.execSelect();
					while (results.hasNext()) {
						QuerySolution soln = results.nextSolution();
						final RDFNode subjectNode = soln.get("s");
						final RDFNode propertyNode = soln.get("p");
						final RDFNode objectNode = soln.get("o");
						Triple t = new Triple(subjectNode.asNode(), propertyNode.asNode(), objectNode.asNode());
						outgoingNeighbors.add(t);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		for (Triple t : outgoingNeighbors) {
			if (!seeds.contains(t.getObject())) {
				stack.push(new Pair<Node, StringBuilder>(t.getObject(),
						new StringBuilder(pop.getValue1().toString()).append("->").append(t.getPredicate().getURI())));
			}
		}

		for (Triple t : incomingNeighbors) {
			stack.push(new Pair<Node, StringBuilder>(t.getSubject(),
					new StringBuilder(pop.getValue1().toString()).append("<-").append(t.getPredicate().getURI())));
		}

	}

	private static void printConfigs() {
		LOG.info("-----------------------------------------------");
		LOG.info("Input data: " + INPUT_DATA);
		LOG.info("Output data: " + OUTPUT_FILE);
		LOG.info("Output data delimiter: " + OUTPUT_FILE_DELIMITER);
		LOG.info("Number of threads: " + NUMBER_OF_THREADS);
		LOG.info("Sampling by percentage: " + SAMPLING_BY_PERCENTAGE);
		LOG.info("Sampling percentage: " + SAMPLING_PERCENTAGE);
		LOG.info("Sampling number: " + SAMPLING_NUMBER);
		LOG.info("Walk depth: " + WALK_DEPTH);
		LOG.info("Serach direction: " + SEARCH_DIRECTION);
		LOG.info("Seed generation sparql: " + SEED_GENERATION_SPARQL);
		LOG.info("Seed generation main variable: " + SEED_GENERATION_MAIN_VARIBALE);
		LOG.info("-----------------------------------------------");
	}

	/**
	 * Return the starting points for doing search
	 * 
	 * @return
	 */
	private static Set<Node> getSeeds(String sparql, String mainVaribaleName) {
		LOG.info("Generating seeds ....");
		long now = System.currentTimeMillis();
		final Set<Node> result = new HashSet<>();
		final ParameterizedSparqlString queryStr = new ParameterizedSparqlString();
		queryStr.append(sparql);
		final Query query = queryStr.asQuery();
		final QueryExecution qexec = QueryExecutionFactory.create(query, model);
		try {
			ResultSet results = qexec.execSelect();
			while (results.hasNext()) {
				QuerySolution soln = results.nextSolution();
				final RDFNode subjectNode = soln.get(mainVaribaleName);
				Node n = subjectNode.asNode();
				result.add(n);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOG.info("Generating seeds is done");
		LOG.info("Generating seeds took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now)
				+ " second");
		return result;
	}

	private static void readDataAndLoadModel() {
		LOG.info("Reading data and loading model ....");
		long now = System.currentTimeMillis();
		File dir = new File(Config.getString("INPUT_DATA", ""));
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File child : directoryListing) {
				String filePath = child.getAbsolutePath();
				try {
					model.read(filePath);
				} catch (Exception e) {
					LOG.error("can not read " + filePath);
					e.printStackTrace();
				}
			}
		}
		LOG.info("Reading data and loading model is done.");
		LOG.info("Reading data and loading model took: "
				+ TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now) + " second");
	}

}
