import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.Comparator;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;


public class AssigTwoz5015906 {

    private final static Logger logger1 = Logger.getLogger("org");
    private final static Logger logger2 = Logger.getLogger("akka");
    private final static String FIELD_SEPARATOR = " ";
    private final static String NODE_WEIGHT_SEPARATOR = ":";
    private final static String PATH_START = "$";
    private final static String EMPTY_LIST = "[]";
    private final static String VISITED = "V";
    private final static String UNVISITED = "U";

    public static class Neighbour implements Serializable {
        String nodeId;
        Integer distanceFromParent;

        Neighbour(String _nodeId, Integer _distanceFromParent) {
            nodeId = _nodeId;
            distanceFromParent = _distanceFromParent;
        }

        String getNodeId() {
            return nodeId;
        }

        Integer getDistanceFromParent() {
            return distanceFromParent;
        }
    }

    static class NodeComparator implements Comparator<Integer>, Serializable {
    // only useful to ensure -1s are at the end of the list
    @Override
    public int compare(Integer t1, Integer t2) {
        if (t1 == -1 || t2 == -1) {
            return t1 == -1 ? 1 : -1;
        }
        return  t1 < t2 ? -1 : 1;
    }
}
    public static class Node implements Serializable, Comparable<Node> {
        String nodeId;
        Integer distanceFromSource;
        ArrayList<Neighbour> adjacencyList;
        String bestPathToNode;
        String visited;

        ArrayList<Neighbour> parseAdjacencyList(String list) {
            ArrayList<Neighbour> result = new ArrayList<>();
            if (list.equals(EMPTY_LIST)) return result;
            String[] cleanList = list
                .replace("[", "")
                .replace("]", "")
                .split(",");

            for (String s : cleanList) {
                String[] pairs = s.split(NODE_WEIGHT_SEPARATOR);
                result.add(new Neighbour(pairs[0], Integer.parseInt(pairs[1])));
            }
            return result;
        }

        Node(String _nodeId, Integer _distanceFromSource, ArrayList<Neighbour> _adjacencyList,
                String _path, String _visited) {
            nodeId = _nodeId;
            distanceFromSource = _distanceFromSource;
            adjacencyList = _adjacencyList;
            bestPathToNode = _path;
            visited = _visited;
        }

        Node(String line) {
            String[] tokens = line.split(" ");
            nodeId = tokens[0];
            distanceFromSource = Integer.parseInt(tokens[1]);
            adjacencyList = parseAdjacencyList(tokens[2]);
            bestPathToNode = tokens[3];
            visited = tokens[4];
        }

        String getNodeId() { return nodeId; }

        Integer getDistanceFromSource() { return distanceFromSource; }

        ArrayList<Neighbour> getAdjacencyList() { return adjacencyList; }

        String getBestPathToNode() { return bestPathToNode; }

        public String getVisited() { return visited; }

        public void markAsVisited() { visited = VISITED; }

        @Override
        public String toString() {
            ArrayList<String> parseList = (ArrayList<String>) adjacencyList.stream()
                .map(n -> String.join(":", n.getNodeId(), n.getDistanceFromParent().toString()))
                .collect(Collectors.toList());

            return String.join(" ",
                nodeId,
                distanceFromSource.toString(),
                parseList.toString().replaceAll("\\s", ""),
                bestPathToNode,
                visited);
        }

        @Override
        public int compareTo(Node node) {
            if (this.distanceFromSource == -1) {
                return 0;
            } else if (node.distanceFromSource == -1) {
                return 1;
            } else {
                return this.distanceFromSource < node.distanceFromSource ? 1 : 0;
            }
        }
    }

    private static JavaRDD<String> formatInput(JavaRDD<String> input, String startNode) {
        return input.mapToPair(line -> {
            String[] tokens = line.split(",");
            String firstNode = tokens[0];
            String lastNode = tokens[1];
            int distance = Integer.parseInt(tokens[2]);
            return new Tuple2<>(firstNode, lastNode + ":" + distance);
        }).groupByKey().map(pair ->
            String.join(FIELD_SEPARATOR,
                pair._1,
                pair._1.equals(startNode) ? "0" : "-1",
                pair._2.toString().replaceAll("\\s", ""),
                PATH_START,
                pair._1.equals(startNode) ? VISITED : UNVISITED
            )
        );
    }

    private static ArrayList<Tuple2<String, Node>> processNeighbours(Node node) {
        ArrayList<Tuple2<String, Node>> result = new ArrayList<>();
        Integer distance = node.getDistanceFromSource();
        if (distance > -1) {
            node.markAsVisited();
            for (Neighbour neighbour : node.getAdjacencyList()) {
                String bestPath = node.getBestPathToNode();
                String newPath = bestPath.equals(PATH_START) ? "" : bestPath + "-";
                // can't view neighbours of neighbours
                ArrayList<Neighbour> neighbours = new ArrayList<>();
                result.add(new Tuple2<>(neighbour.getNodeId(),
                            new Node(neighbour.getNodeId(), distance + neighbour.getDistanceFromParent(),
                            neighbours, newPath + node.getNodeId(), UNVISITED)));
            }
        }
        result.add(new Tuple2<>(node.getNodeId(), node));
        return result;
    }

    private static long discoveredVertexCount(JavaRDD<Node> input) {
        return input.map(node -> node.getVisited())
                .filter(val -> val.equals(VISITED))
                .count();
    }

    private static boolean allNodesVisited(JavaRDD<Node> input) {
        return input.map(node -> node.getVisited())
                .filter(val -> val.equals(UNVISITED))
                .isEmpty();
    }

    public static void main(String[] args) {

        String startNode = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

//        logger1.setLevel(Level.OFF);
//        logger2.setLevel(Level.OFF);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Ass2").setMaster("local"));
        int iterationNo = 0;

        // parse input to desired format
        JavaRDD<String> input = sc.textFile(inputPath);
        formatInput(input, startNode).saveAsTextFile("iteration" + iterationNo);

        boolean allVisited = false;
        long prevDiscoveredVertices = 0;

        while (!allVisited) {
//            System.out.println("Iteration number " + iterationNo);
            JavaRDD<String> vertices = sc.textFile("iteration" + iterationNo);
            iterationNo += 1;

            // mapper (read text and convert back to key pair)
            JavaPairRDD<String, Node> mapper =
                vertices.flatMapToPair(line -> {
                    Node node = new Node(line);
                    ArrayList<Tuple2<String, Node>> valuesToBeEmitted = processNeighbours(node);
                    return valuesToBeEmitted.iterator();
                });

            // reducer (group by keys and take best path/min dist)
            JavaRDD<Node> newVertices = mapper.reduceByKey((node, node2) -> {
                // take adjacency list from the original file (one line of every iteration)
                ArrayList<Neighbour> neighbours = node.getAdjacencyList().isEmpty() ?
                        node2.getAdjacencyList() : node.getAdjacencyList();

                String isVisited = node.getVisited().equals(VISITED) ? VISITED : node2.getVisited();

                // select node with min distance from source
                return node.compareTo(node2) > 0 ?
                        new Node(node.getNodeId(), node.getDistanceFromSource(), neighbours,
                            node.getBestPathToNode(), isVisited)
                        : new Node(node.getNodeId(), node2.getDistanceFromSource(), neighbours,
                            node2.getBestPathToNode(), isVisited);

            }).values();

            newVertices.saveAsTextFile("iteration" + iterationNo);
            allVisited = allNodesVisited(newVertices);

            long discoveredVertices = discoveredVertexCount(newVertices);
            if (prevDiscoveredVertices == discoveredVertices) {
                break;
            } else {
               prevDiscoveredVertices =  discoveredVertices;
            }
        }

        // take last iteration, sort by distance, complete path (by appending final node)
        sc.textFile("iteration" + iterationNo).filter(line -> {
            String[] tokens = line.split(FIELD_SEPARATOR);
            return !tokens[0].equals(startNode);
        }).mapToPair(line -> {
            Node node = new Node(line);
            Tuple2<String, String> nodeAndPath = new Tuple2<>(node.getNodeId(), node.getBestPathToNode());
            return new Tuple2<>(node.getDistanceFromSource(), nodeAndPath);
        }).sortByKey(new NodeComparator()).map(pair -> {
            String node = pair._2._1;
            String path = pair._2._2;
            String distance = pair._1.toString();
            return path.equals(PATH_START) ?
                    String.join(",", node, distance) + ","
                    : String.join(",", node, distance, path + "-" + node);
        }).saveAsTextFile(outputPath);
    }
}
