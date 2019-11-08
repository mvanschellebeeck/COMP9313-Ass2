import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class AssigTwoz5015906 {

    public static class Node implements Serializable {
        Tuple2<String, Integer> node;

        public Node(Tuple2<String, Integer> node) {
            this.node = node;
        }

        public String getName() {
            return node._1;
        }

        public Integer getDistance() {
            return node._2;
        }
    }



    public static class NodeAndNeighbours implements Serializable  {
        String node;
        Iterable<Tuple2<String, Integer>> adjacencyList;

        public NodeAndNeighbours(String node, Iterable<Tuple2<String, Integer>> adjacencyList) {
            this.node = node;
            this.adjacencyList = adjacencyList;
        }

        public String getNodeName() {
            return node;
        }

        public Iterable<Tuple2<String, Integer>> getNeighbours() {
            return adjacencyList;
        }
    }


    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        final String INPUT_FILE = "graph.txt";
        SparkConf conf = new SparkConf()
                .setAppName("Ass2")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(INPUT_FILE);



        String startNode = "N0";

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> parsed = input.mapToPair(
                line -> {
                    String [] parts = line.split(",");
                    String firstNode = parts[0];
                    String lastNode = parts[1];
                    Integer distance = Integer.parseInt(parts[2]);
                    return new Tuple2<>(
                            firstNode,
                            new Tuple2<>(lastNode, distance)
                    );
                }
        ).groupByKey();

        HashMap<String, Boolean> visited = new HashMap<>();
        boolean start = true;

        while (visited.size() != 6) {


            if (!start)  {
                JavaRDD<String> myInput = sc.textFile("wasup");
                JavaPairRDD<String, Tuple3<String, Integer, String>> abc = myInput.mapToPair(
                        line -> {
                           String []  parts = line.split(",");
                           String node = parts[0];
                           String status = parts[1];
                           Integer dist = Integer.parseInt(parts[2]);
                           String path = parts[3];

                           return new Tuple2<String, Tuple3<String, Integer, String>>(
                               node,
                               new Tuple3(status, dist, path)
                           );
                        }
                );

                break;
            }

            parsed.collect().forEach(System.out::println);

            // mapper
            JavaPairRDD<String, Tuple3<String, Integer, String>> adjacentNodes = parsed.flatMapToPair(pair -> {

                NodeAndNeighbours nodeAndNeighbours = new NodeAndNeighbours(pair._1, pair._2);
                Iterable<Tuple2<String, Integer>> nodes = nodeAndNeighbours.getNeighbours();

                ArrayList<Tuple2<String, Tuple3<String, Integer, String>>> ret = new ArrayList<>();

                nodes.forEach(node -> {
                    String nodeName = node._1;
                    if (nodeName.equals(startNode))
                        ret.add(new Tuple2<String, Tuple3<String, Integer, String>>(
                                nodeName,
                                new Tuple3<>("Y", 0, "path")));
                    else
                        ret.add(new Tuple2<String, Tuple3<String, Integer, String>>(
                                nodeName,
                                new Tuple3<>("N", -1, "path")));
                });

                return ret.iterator();
            });


            // reducer
           adjacentNodes
                   .reduceByKey( (best, curr) -> curr._2() < best._2() ? curr : best)
                   // comma separated - makes it easier to read on next iteration
                   .map(a -> a.toString().replace("(","").replace(")", ""))
                   .saveAsTextFile("wasup");

            visited.put(startNode, true);

            start = false;
        }
    }
}
