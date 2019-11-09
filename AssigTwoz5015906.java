import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Serializable;
import scala.Tuple2;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class AssigTwoz5015906 {

    private final static Logger logger1 = Logger.getLogger("org");
    private final static Logger logger2 = Logger.getLogger("akka");
    private final static String BAR_SEPARATOR = "|";
    private final static String BAR_SEAPRATOR_SPLIT = "\\" + BAR_SEPARATOR;
    private final static String NODE_WEIGHT_SEPARATOR = ":";
    private final static String PATH_START = "$";

    // enum for index of lists?

    public static class Node implements Serializable, Comparable<Node> {
        String nodeId;
        Integer distanceFromSource;
        ArrayList<String> adjacencyList;
        String bestPathToNode;

        public ArrayList<String> parseAdjacencyList(String list) {
            ArrayList<String> result = new ArrayList<>();
            if (list.equals("[]")) return result;
            String[] cleanList = list
                    .replace("[", "")
                    .replace("]", "")
                    .split(",");

           for (int i = 0; i < cleanList.length; i++) {
                String[] pairs = cleanList[i].split(NODE_WEIGHT_SEPARATOR);
                result.add(pairs[0] + ":" + pairs[1]);
            }
           return result;
        }

        Node(String nodeId, Integer distanceFromSource,
             ArrayList<Tuple2<String, Integer>> adjacencyList, String path) {
            this.nodeId = nodeId;
            this.distanceFromSource = distanceFromSource;
            this.adjacencyList = (ArrayList<String>) adjacencyList
                    .stream()
                    .map(t -> String.join(":", t._1, t._2.toString()))
                    .collect(Collectors.toList());
            this.bestPathToNode = path;
        }

        Node(String line) {
            String[] tokens = line.split(BAR_SEAPRATOR_SPLIT);
            nodeId = tokens[0];
            distanceFromSource = Integer.parseInt(tokens[1]);
            adjacencyList = parseAdjacencyList(tokens[2]);
            bestPathToNode = tokens[3];
        }

        String getNodeId() {
            return nodeId;
        }

        Integer getDistanceFromSource() {
            return distanceFromSource;
        }

        ArrayList<Tuple2<String, Integer>> getAdjacencyList() {
            return (ArrayList<Tuple2<String, Integer>>) adjacencyList.stream().map(s -> {
               String[] tokens = s.split(":") ;
               return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
            }).collect(Collectors.toList());
        }

        public String getBestPathToNode() {
            return bestPathToNode;
        }

        @Override
        public String toString() {
            return String.join("|",
                    nodeId,
                    distanceFromSource.toString(),
                    adjacencyList.toString().replaceAll("\\s", ""),
                    bestPathToNode);
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
        // change to
        // Node   DistanceToSource Neighbours           Path
        // NO     0                [(N1,4),(N2,3)]      ""
        // N1     -1                [(N1,4),(N2,3)]      ""
        // ...
        return input.mapToPair(
                line -> {
                    String [] parts = line.split(",");
                    String firstNode = parts[0];
                    String lastNode = parts[1];
                    Integer distance = Integer.parseInt(parts[2]);
                    return new Tuple2<>(
                            firstNode,
                            lastNode + ":" + distance
                    );
                }
        ).groupByKey().map(pair ->
            String.join(BAR_SEPARATOR,
                    pair._1,
                    pair._1.equals(startNode) ? "0" : "-1",
                    pair._2.toString().replaceAll("\\s", ""),
                    PATH_START
            )
        );
    }



    public static void main(String[] args) {


        String startNode = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        logger1.setLevel(Level.OFF);
        logger2.setLevel(Level.OFF);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Ass2").setMaster("local"));
        JavaRDD<String> input = sc.textFile(inputPath);

        JavaRDD<String> parsed = formatInput(input, startNode);

        JavaRDD<Node> graph = parsed.mapToPair(line -> {
            Node node = new Node(line);
            return new Tuple2<>(node.getNodeId(), node);
        }).values();

        graph.collect().forEach(System.out::println);
        graph.saveAsTextFile("iteration0");

        HashMap<Integer, Boolean> visited = new HashMap<>();
        int index = 0;

        while (visited.size() != 6) {
            JavaRDD<String> prevGraph = sc.textFile("iteration" + index);
            index += 1;
            System.out.println("Previous iteration:");
            prevGraph.collect().forEach(System.out::println);

            // mapper (read text and convert back to key pair)
            JavaPairRDD<String, Node> mapper =
                    prevGraph
                    .flatMapToPair(line-> {
                        ArrayList<Tuple2<String, Node>> result = new ArrayList<>();
                        Node node = new Node(line);
                        result.add(new Tuple2<>(node.getNodeId(), node));
                        if (node.getDistanceFromSource() >= 0) {
                            Integer distance = node.getDistanceFromSource();
                            for (Tuple2<String, Integer> neighbour : node.getAdjacencyList()) {
                                result.add(new Tuple2<>(neighbour._1,
                                        new Node(neighbour._1, distance + neighbour._2, new ArrayList<>(),
                                                (node.getBestPathToNode().equals(PATH_START) ? "" : node.getBestPathToNode() + "-") + node.getNodeId())));
                            }
                        }
                        return result.iterator();
                    });

            mapper.reduceByKey((node, node2) ->{
                // take adjacency list from the original file (one line of every iteration)
                ArrayList<Tuple2<String, Integer>> neighbours = node.getAdjacencyList().isEmpty() ?
                        node2.getAdjacencyList() : node.getAdjacencyList();

                Integer minDistance;
                String path;

               if (node.compareTo(node2) == 1) {
                  minDistance = node.getDistanceFromSource();
                  path = node.getBestPathToNode();
                } else {
                   minDistance = node2.getDistanceFromSource();
                   path = node2.getBestPathToNode();
               }

                return new Node(node.getNodeId(), minDistance, neighbours, path);
            }).values().saveAsTextFile("iteration" + (index));

            visited.put(index, true);
        }

                sc.textFile("iteration" + index).filter(line -> {
            String[] tokens = line.split(BAR_SEAPRATOR_SPLIT);
            return !tokens[0].equals(startNode);
        }).map(line ->
            {
                String[] tokens = line.split(BAR_SEAPRATOR_SPLIT);
                String finishNode = tokens[0];
                String distance = tokens[1];
                String path = tokens[3];

                return String.join(",", finishNode, distance,  path);
            }
        ).mapToPair(line -> {
            String[] tokens = line.split(",");
            Integer distance = Integer.parseInt(tokens[1]);
            Tuple2<String, String> nodeAndPath = new Tuple2<>(tokens[0], tokens[2]);
            return new Tuple2<>(distance, nodeAndPath);
        }).sortByKey().map(pair -> {
            String node = pair._2._1;
            String path = pair._2._2;
            Integer distance = pair._1;
            return path.equals(PATH_START) ? String.join(",", node, distance.toString())
                    : String.join(",", node, distance.toString(), path + "-" + node);
        }).saveAsTextFile(outputPath);


    }
}
