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
    private final static String INPUT_FILE = "graph.txt";
    private final static String BAR_SEPARATOR = "|";
    private final static String BAR_SEAPRATOR_SPLIT = "\\" + BAR_SEPARATOR;
    private final static String NODE_WEIGHT_SEPARATOR = ":";
    private final static String NO_PATH="XX";

    // enum for index of lists?

    public static class Node implements Serializable {
        String nodeId;
        Integer distanceFromSource;
        ArrayList<String> adjacencyList;
        String bestPathToNode;

        public ArrayList<String> parseAdjacencyList(String list) {
            ArrayList<String> result = new ArrayList<>();
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
            this.adjacencyList = new ArrayList<>();
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
                    NO_PATH
            )
        );
    }



    public static void main(String[] args) {

        logger1.setLevel(Level.OFF);
        logger2.setLevel(Level.OFF);

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Ass2").setMaster("local"));
        JavaRDD<String> input = sc.textFile(INPUT_FILE);

        String startNode = "N0";
        JavaRDD<String> parsed = formatInput(input, startNode);

        JavaRDD<String> graph = parsed.map(line -> {
            Node node = new Node(line);
            return node.toString();
        });

        graph.collect().forEach(System.out::println);
        graph.saveAsTextFile("iteration0");

        HashMap<Integer, Boolean> visited = new HashMap<>();
        int index = 0;

        while (visited.size() != 6) {
            JavaRDD<String> prevGraph = sc.textFile("iteration" + index);
            System.out.println("Previous iteration:");
            prevGraph.collect().forEach(System.out::println);

            // mapper
            JavaPairRDD<String, Node> mapper =
                    prevGraph
                    // only work with nodes that have a distance (temporarily)
                    .filter(v -> Integer.parseInt(v.split(BAR_SEAPRATOR_SPLIT)[1]) >= 0)
                    .flatMapToPair(line-> {
                        ArrayList<Tuple2<String, Node>> result = new ArrayList<>();
                        Node node = new Node(line);
                        result.add(new Tuple2<>(node.getNodeId(), node));
                        Integer distance = node.getDistanceFromSource();
                        for(Tuple2<String, Integer> neighbour : node.getAdjacencyList()) {
                            result.add(new Tuple2<>(neighbour._1,
                                    new Node(neighbour._1, distance + neighbour._2, new ArrayList<>(),
                                            node.getNodeId())));
                        }
                        return result.iterator();
                    });

            mapper.reduceByKey((node, node2) ->{
                ArrayList<Tuple2<String, Integer>> neighbours = node.getAdjacencyList().isEmpty() ?
                        node2.getAdjacencyList() : node.getAdjacencyList();

                Integer nodeDistance = node.getDistanceFromSource();
                Integer node2Distance = node2.distanceFromSource;
                String path;
                Integer minDistance;
                if (nodeDistance < node2Distance) {
                   minDistance = nodeDistance;
                   path = node.getBestPathToNode();
                } else {
                   minDistance = node2Distance;
                   path = node2.getBestPathToNode();
                }

                return new Node(node.getNodeId(), minDistance, neighbours, path);
            }).collect().forEach(System.out::println);


            break;

        }

//            // reducer
//           adjacentNodes
//                   .reduceByKey( (best, curr) -> curr._2() < best._2() ? curr : best)
//                   // comma separated - makes it easier to read on next iteration
//                   .map(a -> a.toString().replace("(","").replace(")", ""))
//                   .saveAsTextFile("wasup");
//
//            visited.put(startNode, true);
//
//            start = false;
//        }
    }
}
