package model;

import org.apache.jena.graph.Node;

import util.Counter;

/**
 * A wrapper for {@link Node} to keep more information about the nodes and the
 * search process
 * 
 * @author fbm
 *
 */
public class GraphNode {
	private long id;
	private final Node node;
	private String path;
	private boolean visited;
	private long depth;

	public GraphNode(Node node) {
		this.id = Counter.getInstance().getNext();
		this.node = node;
		this.path = "";
		this.visited = false;
		this.depth = 0;
	}

	/**
	 * @return the visited
	 */
	public boolean isVisited() {
		return visited;
	}

	/**
	 * @param visited the visited to set
	 */
	public void setVisited(boolean visited) {
		this.visited = visited;
	}

	/**
	 * @return the depth
	 */
	public long getDepth() {
		return depth;
	}

	/**
	 * @param depth the depth to set
	 */
	public void setDepth(long depth) {
		this.depth = depth;
	}

	/**
	 * @param path the path to set
	 */
	public void setPath(String path) {
		this.path = path;
	}

	/**
	 * @return the id
	 */
	public long getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(long id) {
		this.id = id;
	}

	/**
	 * @return the node
	 */
	public Node getNode() {
		return node;
	}

	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}

	@Override
	public String toString() {
		return "GraphNode [id=" + id + ", node=" + node + ", path=" + path + "]";
	}

}
