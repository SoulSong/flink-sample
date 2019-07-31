package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import akka.japi.tuple.Tuple3;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 10:35
 */
public class CrossFunctionSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Coord> coords1 = env.fromElements(new Coord(1, 1, 1), new Coord(2, 2, 2));
        coords1.print();
        DataSet<Coord> coords2 = env.fromElements(new Coord(3, 3, 3), new Coord(4, 4, 4));
        coords2.print();

        /*
         * <pre>
         *                                                  Tuple3(1,3,2.8284271247461903)
         *      Coord(1,1,1)                Coord(3,3,3)    Tuple3(1,4,4.242640687119285)
         *                  crossFunction                   Tuple3(2,3,1.4142135623730951)
         *      Coord(2,2,2)                Coord(4,4,4)    Tuple3(2,4,2.8284271247461903)
         * </pre>
         */
        DataSet<Tuple3<Integer, Integer, Double>> distances =
                coords1.cross(coords2)
                        // apply CrossFunction
                        .with(new EuclideanDistComputer());
        distances.print();


    }

    /**
     * CrossFunction computes the Euclidean distance between two Coord objects.
     */
    public static class EuclideanDistComputer
            implements CrossFunction<Coord, Coord, Tuple3<Integer, Integer, Double>> {

        @Override
        public Tuple3<Integer, Integer, Double> cross(Coord c1, Coord c2) {
            // compute Euclidean distance of coordinates
            double dist = sqrt(pow(c1.x - c2.x, 2) + pow(c1.y - c2.y, 2));
            return new Tuple3<>(c1.id, c2.id, dist);
        }
    }

    static class Coord {
        public int id;
        public int x;

        public int y;

        public Coord(int id, int x, int y) {
            this.id = id;
            this.x = x;
            this.y = y;
        }
        @Override
        public String toString() {
            return "Coord(" + id +
                    "," + x +
                    "," + y +
                    ')';
        }

    }

}
