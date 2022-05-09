import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

import javax.management.relation.Relation;
import java.util.*;

import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.sort_array;

public class FindPath {
    // From: https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
    private static double distance(double lat1, double lat2, double lon1, double lon2) {
        final int R = 6371; // Radius of the earth
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters
        double height = 0; // For this assignment, we assume all locations have the same height.
        distance = Math.pow(distance, 2) + Math.pow(height, 2);
        return Math.sqrt(distance);
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("create_graph");

        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> tempNode = session.read()
                .format("xml")
                .option("rowTag", "node")
                .load(args[0]);

        Dataset<Row> tempWay = session.read()
                .format("xml")
                .option("rowTag", "way")
                .load(args[0]);

        Dataset<Row> dsNode = tempNode.select("_id");

        Dataset<Row> dsWay = tempWay.select("nd");

        StringBuilder sb = new StringBuilder();
        List<String> dataList = new ArrayList<>();

        Dataset<Row> edges = tempWay.select("nd._ref", "tag._k", "tag._v")
                .as(Encoders.bean(Way.class))
                .flatMap((FlatMapFunction<Way, Row>) way -> {

                })
                .agg(array_distinct(sort_array(collect_list("end"))));
//        Dataset<Row> adjLst = dsWay.orderBy("_src").groupBy("_src")
//                .agg(functions.sort_array(functions.collect_set("_dst")))
//                .toDF("_src", "_dst");

//        adjLst.show();
//        tempWay.show();
//        tempNode.show();
        dsNode.show();
//        dsWay.show();

        dsNode.javaRDD().map(x -> x.toString()).saveAsTextFile(args[2]);



        dataList.add("1534829927 -> 1534829925");
        dataList.add("1533913917 -> 4636541618 -> 4636541619");
        dataList.add("6977414038 -> 6302941964 -> 6977505422 -> 6977505423");
        dataList.add("1534829928 -> 1534829926 -> 1534822534 -> 1534822535");
        dataList.add("1534830314 -> 1534830321 -> 1534830318 -> 1534822499 -> 1534822530");

        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<String> dataDs = spark.createDataset(dataList, Encoders.STRING());
        Dataset<String> dataListDs = spark.createDataset(dataList,    Encoders.STRING());
        //dataDs.show();

        dataDs.javaRDD().saveAsTextFile(args[3]);

//        List<Long> list = new ArrayList<>();
//
//        HashMap<Row, List<Long>> storeMap = new HashMap<>();
//
//        List<Row> tempList = dsNode.collectAsList();
//
//        for (Row r : tempList) {
//            storeMap.put(r, new ArrayList<>());
//        }

//        for (Row r: dsWay) {
//            r.getList();
//        }


//        List<User> users = new ArrayList<>();
//        users.add(new User(1L, "John"));
//        users.add(new User(2L, "Martin"));
//        users.add(new User(3L, "Peter"));
//        users.add(new User(4L, "Alicia"));
//
//        List<Relationship> relationships = new ArrayList<>();
//        relationships.add(new Relationship("Friend", "1", "2"));
//        relationships.add(new Relationship("Following", "1", "4"));
//        relationships.add(new Relationship("Friend", "2", "4"));
//        relationships.add(new Relationship("Relative", "3", "1"));
//        relationships.add(new Relationship("Relative", "3", "4"));
//
//        Dataset<Row> userDataset = session.createDataFrame(users, User.class);
//        Dataset<Row> relationshipDataset = session.createDataFrame(relationships, Relation.class);
//
//
//        graph.vertices().show();
//        graph.edges().show();
//
        session.stop();
    }

    public static class Way {
        Way()
    }

//    public static class Relationship implements Serializable {
//        private String type;
//        private String src;
//        private String dst;
//        private UUID id;
//
//        public Relationship(String type, String src, String dst) {
//            this.type = type;
//            this.src = src;
//            this.dst = dst;
//            this.id = UUID.randomUUID();
//        }
//        // getters and setters
//    }
//
//    public static class User {
//        private Long id;
//        private String name;
//
//        User(Long id, String name) {
//            this.id = id;
//            this.name = name;
//        }
//
//    }

}

