package main;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import org.javatuples.Triplet;

import model.GraphNode;
import util.Config;

public class AnalyzeRecursive {

	private static final Logger LOG = Logger.getLogger(AnalyzeRecursive.class.getCanonicalName());
	private static final String INPUT_DATA = Config.getString("INPUT_DATA", ";");
	private static final String OUTPUT_FILE = Config.getString("OUTPUT_FILE", "output.tsv");
	private static final float SAMPLING_PERCENTAGE = Config.getFloat("SAMPLING_PERCENTAGE", 1f);
	private static final int SAMPLING_NUMBER = Config.getInt("SAMPLING_NUNBER", 5);
	private static final int WALK_DEPTH = Config.getInt("WALK_DEPTH", 5);
	private static final int WALK_DEPTH_UP = Config.getInt("WALK_DEPTH_UP", 2);
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

	public static void main(String[] args) {
		printConfigs();
		readDataAndLoadModel();
		LOG.info("-----------------------------------------------");
		Set<GraphNode> seeds = getSeeds(SEED_GENERATION_SPARQL, SEED_GENERATION_MAIN_VARIBALE).stream()
				.map(p -> new GraphNode(p)).collect(Collectors.toSet());
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
		final Query query = generateSparqlQuary();
		LOG.info("-----------------------------------------------");
		executeSparqlAndWriteOutput(query);

	}

	private static ResultSet executeSparql(Query query) {
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		return qexec.execSelect();
	}

	private static void executeSparqlAndWriteOutput(Query query) {
		try {
			LOG.info("Executing sparql query  ....");
			long now = System.currentTimeMillis();
			ResultSet results = executeSparql(query);
			LOG.info("Executing sparql query is done.");
			LOG.info("Executing Sparql took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now)
					+ " second");
			LOG.info("-----------------------------------------------");
			final TSVOutput csv = new TSVOutput();
			now = System.currentTimeMillis();
			LOG.info("Saving Sparql result ....");
			csv.format(new FileOutputStream(OUTPUT_FILE), results);
			LOG.info("Saving Sparql result is done.");
			LOG.info("Saving Sparql result to output file took: "
					+ TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - now) + " second");
		} catch (Exception e) {
			LOG.error(e.getMessage(),e);
		}
	}

	private static Query generateSparqlQuary() {

		final List<Triplet<String, String, String>> seedGenerationVariables = new LinkedList<>();
		// TODO: how to handle this automatically
		seedGenerationVariables.add(
				new Triplet<String, String, String>("?accident", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
						"<http://www.engie.fr/ontologies/accidentontology/RoadAccident>"));
		// get seed generate sparql and extract triples out of it
		// put them inside variables one by one
		// this is the first mandatory building block

		final ParameterizedSparqlString queryStr = new ParameterizedSparqlString();
		final LinkedHashSet<String> onlyVariableNames = new LinkedHashSet<>();
		onlyVariableNames.add(SEED_GENERATION_MAIN_VARIBALE);

		final List<String> optionalBlocks = new ArrayList<>();
		for (String line : uniqueFeatures) {
			optionalBlocks.add(extractSparqlFromSingleLineFeatures(line, onlyVariableNames));
		}

		queryStr.append("SELECT ");
		for (String s : onlyVariableNames) {
			queryStr.append(s + " ");
		}
		queryStr.append(" WHERE {");

		for (Triplet<String, String, String> mandatoryTriples : seedGenerationVariables) {
			queryStr.append(mandatoryTriples.getValue0());
			queryStr.append(" ");
			queryStr.append(mandatoryTriples.getValue1());
			queryStr.append(" ");
			queryStr.append(mandatoryTriples.getValue2());
			queryStr.append(". ");
		}

		for (String optionalBlock : optionalBlocks) {
			queryStr.append(optionalBlock);
		}
		queryStr.append("}");
		LOG.info(queryStr);
		return queryStr.asQuery();

	}

	private static String extractSparqlFromSingleLineFeatures(String line, LinkedHashSet<String> onlyVariableNames) {
		final String[] split = line.split("->");
		if (split[0].trim().isEmpty()) {
			// only going deep
			return extractSparqlFromSingleLineFeaturesOnlyGoingDeep(line, onlyVariableNames,
					SEED_GENERATION_MAIN_VARIBALE, true, null);
		} else {
			// we may go up
			return extractSparqlFromSingleLineFeaturesOnlyUpAndGoingDeep(line, onlyVariableNames);
		}
	}

	private static String extractSparqlFromSingleLineFeaturesOnlyUpAndGoingDeep(String line,
			LinkedHashSet<String> onlyVariableNames) {
		final List<Triplet<String, String, String>> localVaribales = new LinkedList<>();

		final String[] pathes = line.split("->");
		final String[] upwardNodes = pathes[0].split("<-");

		String var = "";
		String prefix = "";
		for (int i = 1; i < upwardNodes.length; i++) {
			String path = "<" + upwardNodes[i] + ">";
			final String[] properties = upwardNodes[i].split("/");
			String variableName;
			variableName = ("?" + properties[properties.length - 1]).replace("#", "_").replace("-", "_");
			variableName = ("?" + generateVaribaleNameFromFullPath(null, upwardNodes, i)).replace("#", "_").replace("-",
					"_");

			if (var.isEmpty()) {
				var = variableName;
				localVaribales
						.add(new Triplet<String, String, String>(variableName, path, SEED_GENERATION_MAIN_VARIBALE));
			} else {
				localVaribales.add(new Triplet<String, String, String>(variableName, path, var));
				var = variableName;
			}

		}

		prefix = generateVaribaleNameFromFullPath(null, upwardNodes, upwardNodes.length - 1) + "_";
		final String goingDeepResult = extractSparqlFromSingleLineFeaturesOnlyGoingDeep(line.split("->", 2)[1],
				onlyVariableNames, var, false, prefix);

		final StringBuilder result = new StringBuilder();
		result.append(" OPTIONAL {");
		result.append(goingDeepResult);
		for (Triplet<String, String, String> t : localVaribales) {
			result.append(t.getValue0().replace("?" + UNIQUE_STRING_TOKEN, "?"));
			result.append(" ");
			result.append(t.getValue1().replace("?" + UNIQUE_STRING_TOKEN, "?"));
			result.append(" ");
			result.append(t.getValue2().replace("?" + UNIQUE_STRING_TOKEN, "?"));
			result.append(" . ");
		}
		result.append(" } ");

		return result.toString();
	}

	private static String extractSparqlFromSingleLineFeaturesOnlyGoingDeep(String line,
			LinkedHashSet<String> onlyVariableNames, String startPoint, boolean withOptional, String prefix) {
		// final List<Triplet<String, String, String>> variables = new LinkedList<>();
		final List<Triplet<String, String, String>> localVaribales = new LinkedList<>();
		final String[] pathes = line.split("->");
		String var = "";
		for (int i = 0; i < pathes.length; i++) {
			if (pathes[i].trim().isEmpty()) {
				continue;
			}
			String path = "<" + pathes[i] + ">";
			final String[] properties = pathes[i].split("/");
			String variableName;
			if (i == pathes.length - 1) {
				variableName = ("?" + UNIQUE_STRING_TOKEN + properties[properties.length - 1]).replace("#", "_")
						.replace("-", "_");
				variableName = ("?" + UNIQUE_STRING_TOKEN + generateVaribaleNameFromFullPath(prefix, pathes, i))
						.replace("#", "_").replace("-", "_");
			} else {
				variableName = ("?" + properties[properties.length - 1]).replace("#", "_").replace("-", "_");
				variableName = ("?" + generateVaribaleNameFromFullPath(prefix, pathes, i)).replace("#", "_")
						.replace("-", "_");
			}

			if (var.isEmpty()) {
				var = variableName;
				localVaribales.add(new Triplet<String, String, String>(startPoint, path, variableName));
			} else {
				localVaribales.add(new Triplet<String, String, String>(var, path, variableName));
				var = variableName;
			}
		}
		localVaribales.forEach(p -> {
			if (p.getValue0().contains("?" + UNIQUE_STRING_TOKEN)) {
				onlyVariableNames.add(p.getValue0().replace("?" + UNIQUE_STRING_TOKEN, "?"));
			}
			if (p.getValue2().contains("?" + UNIQUE_STRING_TOKEN)) {
				onlyVariableNames.add(p.getValue2().replace("?" + UNIQUE_STRING_TOKEN, "?"));
			}

		});

		final StringBuilder result = new StringBuilder();
		if (withOptional) {
			result.append(" OPTIONAL {");
		}
		for (Triplet<String, String, String> t : localVaribales) {
			result.append(t.getValue0().replace("?" + UNIQUE_STRING_TOKEN, "?"));
			result.append(" ");
			result.append(t.getValue1().replace("?" + UNIQUE_STRING_TOKEN, "?"));
			result.append(" ");
			result.append(t.getValue2().replace("?" + UNIQUE_STRING_TOKEN, "?"));
			result.append(" . ");
		}
		if (withOptional) {
			result.append(" } ");
		}

		return result.toString();
	}

	private static String generateVaribaleNameFromFullPath(String prefix, String[] pathes, int threshold) {
		final StringBuilder result = new StringBuilder();
		result.append(prefix == null ? "" : prefix);
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

	private static void runSearchWithPercentage(final Set<GraphNode> seeds) {
		LOG.info("Genering features by doing graph serach ...");
		long now = System.currentTimeMillis();
		try {
			for (GraphNode seed : seeds) {
				if (Math.random() <= SAMPLING_PERCENTAGE) {
					executor.submit(dfs(seed));
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

	private static void runSearchWithNumbers(final Set<GraphNode> seeds) {
		LOG.info("Genering features by doing graph serach ...");
		long now = System.currentTimeMillis();
		try {
			int count = 0;
			for (GraphNode seed : seeds) {
				executor.submit(dfs(seed));
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

	private static Runnable dfs(GraphNode node) {
		return () -> {
			if (SEARCH_DIRECTION == SearchDirection.OUT_GOING) {
				dfsRecursive(node, "");
			} else if (SEARCH_DIRECTION == SearchDirection.IN_COMING) {
				List<GraphNode> parents = new ArrayList<GraphNode>();
				getParentsNodes(node, WALK_DEPTH_UP, parents);
				for (GraphNode n : parents) {
					dfsRecursive(n, n.getPath());
				}
			} else if (SEARCH_DIRECTION == SearchDirection.BOTH) {
				dfsRecursive(node, "");
				List<GraphNode> parents = new ArrayList<GraphNode>();
				getParentsNodes(node, WALK_DEPTH_UP, parents);
				for (GraphNode n : parents) {
					dfsRecursive(n, n.getPath());
				}
			}
			LOG.info("Node " + node.getNode() + " is traversed.");
		};

	}

	private static List<GraphNode> getParentsNodes(GraphNode node, long currentDepth, List<GraphNode> list) {

		if (currentDepth == 0) {
			return list;
		}

		final List<Triple> neighbors = new ArrayList<>();
		ParameterizedSparqlString queryStr = new ParameterizedSparqlString();
		queryStr.append("SELECT ?s ?p ?o WHERE ");
		queryStr.append("{");
		queryStr.append("?s ?p ?o .");
		queryStr.append("FILTER (?o = ");
		queryStr.appendNode(node.getNode());
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
				neighbors.add(t);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		final List<GraphNode> parents = neighbors.stream().map(p -> {
			GraphNode newNode = new GraphNode(p.getSubject());
			newNode.setPath(node.getPath() + "<-" + p.getPredicate().toString());
			return newNode;
		}).collect(Collectors.toList());
		list.addAll(parents);
		parents.forEach(p -> getParentsNodes(p, currentDepth - 1, list));
		return list;
	}

	private static void dfsRecursive(GraphNode node, String parentPath) {
		node.setVisited(true);
		if (node.getNode().isLiteral()) {
			uniqueFeatures.add(node.getPath());
		} else {
			goDeep(node, parentPath);
		}

	}

	private static void goDeep(GraphNode node, String parentPath) {
		final String nodeString = node.getNode().toString();
		final List<Triple> outgoingNeighbors = new ArrayList<>();
		if (node.getNode().isBlank()) {
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
			queryStr.appendNode(node.getNode());
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

		List<GraphNode> neighbors = outgoingNeighbors.stream().map(p -> {
			GraphNode newNode = new GraphNode(p.getObject());
			newNode.setPath(parentPath + "->" + p.getPredicate().toString());
			newNode.setDepth(node.getDepth() + 1);
			return newNode;
		}).collect(Collectors.toList());

		for (GraphNode n : neighbors) {
			if (!n.isVisited() && n.getDepth() <= WALK_DEPTH) {
				dfsRecursive(n, n.getPath());
			}
		}
	}

	private static void printConfigs() {
		LOG.info("-----------------------------------------------");
		LOG.info("Input data: " + INPUT_DATA);
		LOG.info("Output data: " + OUTPUT_FILE);
		LOG.info("Number of threads: " + NUMBER_OF_THREADS);
		LOG.info("Sampling by percentage: " + SAMPLING_BY_PERCENTAGE);
		LOG.info("Sampling percentage: " + SAMPLING_PERCENTAGE);
		LOG.info("Sampling number: " + SAMPLING_NUMBER);
		LOG.info("Walk depth: " + WALK_DEPTH);
		LOG.info("Walk upward depth: " + WALK_DEPTH_UP);
		LOG.info("Search direction: " + SEARCH_DIRECTION);
		LOG.info("Seed generation sparql: " + SEED_GENERATION_SPARQL);
		LOG.info("Seed generation main variable: " + SEED_GENERATION_MAIN_VARIBALE);
		LOG.info("-----------------------------------------------");
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
}
