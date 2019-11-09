import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Serializable;
import scala.Tuple2;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class AssigTwoz5015906 {

    final static Logger logger1 = Logger.getLogger("org");
    final static Logger logger2 = Logger.getLogger("akka");
    final static String INPUT_FILE = "graph.txt";
    final static String BAR_SEPARATOR = "|";
    final static String NODE_WEIGHT_SEPARATOR = ":";

    public static class Node implements Serializable {
        String nodeId;
        Integer distanceToSource;
        ArrayList<Tuple2<String,Integer>> adjacencyList;
        String bestPathToNode;

        public ArrayList<Tuple2<String,Integer>> parseAdjacencyList(String list) {
            ArrayList<Tuple2<String,Integer>> result = new ArrayList<>();
           String[] cleanList = list
                    .replace("[", "")
                    .replace("]", "")
                    .split(",");

           for (int i = 0; i < cleanList.length; i++) {
                String[] pairs = cleanList[i].split(NODE_WEIGHT_SEPARATOR);
                result.add(new Tuple2<>(
                            pairs[0],
                            Integer.parseInt(pairs[1])
                        )
                );
           }
           return result;
        }

        public Node(String line) {
            String[] tokens = line.split(BAR_SEPARATOR);
            System.out.println(tokens);
            nodeId = tokens[0];
            distanceToSource = Integer.parseInt(tokens[1]);
            adjacencyList = parseAdjacencyList(tokens[2]);
            bestPathToNode = tokens[3];
        }

        public String getNodeId() {
            return nodeId;
        }

        public Integer getDistanceToSource() {
            return distanceToSource;
        }

        public ArrayList<Tuple2<String, Integer>> getAdjacencyList() {
            return adjacencyList;
        }

        public String getBestPathToNode() {
            return bestPathToNode;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "nodeId='" + nodeId + '\'' +
                    ", distanceToSource=" + distanceToSource +
                    ", adjacencyList=" + adjacencyList +
                    ", bestPathToNode='" + bestPathToNode + '\'' +
                    '}';
        }
    }

    public static JavaRDD<String> formatInput(JavaRDD<String> input, String startNode) {
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
                    ""
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

        JavaPairRDD<String, String> tester = parsed.flatMapToPair(line -> {
            Node node = new Node(line);
            ArrayList<Tuple2<String, String>> result = new ArrayList<>();;
            result.add(new Tuple2<>(node.getNodeId(), node.toString()));
            return result.iterator();
        });


        parsed.collect().forEach(System.out::println);

        HashMap<String, Boolean> visited = new HashMap<>();
        boolean start = true;



        // mapper



//        while (visited.size() != 6) {
//
//
//            if (!start)  {
//                JavaRDD<String> myInput = sc.textFile("wasup");
//                JavaPairRDD<String, Tuple3<String, Integer, String>> abc = myInput.mapToPair(
//                        line -> {
//                           String []  parts = line.split(",");
//                           String node = parts[0];
//                           String status = parts[1];
//                           Integer dist = Integer.parseInt(parts[2]);
//                           String path = parts[3];
//
//                           return new Tuple2<String, Tuple3<String, Integer, String>>(
//                               node,
//                               new Tuple3(status, dist, path)
//                           );
//                        }
//                );
//
//                break;
//            }
//
//            parsed.collect().forEach(System.out::println);

            // mapper
//            JavaPairRDD<String, Tuple3<String, Integer, String>> adjacentNodes = parsed.flatMapToPair(pair -> {
//
//                NodeAndNeighbours nodeAndNeighbours = new NodeAndNeighbours(pair._1, pair._2);
//                Iterable<Tuple2<String, Integer>> nodes = nodeAndNeighbours.getNeighbours();
//
//                ArrayList<Tuple2<String, Tuple3<String, Integer, String>>> ret = new ArrayList<>();
//
//                nodes.forEach(node -> {
//                    String nodeName = node._1;
//                    if (nodeName.equals(startNode))
//                        ret.add(new Tuple2<String, Tuple3<String, Integer, String>>(
//                                nodeName,
//                                new Tuple3<>("Y", 0, "path")));
//                    else
//                        ret.add(new Tuple2<String, Tuple3<String, Integer, String>>(
//                                nodeName,
//                                new Tuple3<>("N", -1, "path")));
//                });
//
//                return ret.iterator();
//            });
//
//
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
