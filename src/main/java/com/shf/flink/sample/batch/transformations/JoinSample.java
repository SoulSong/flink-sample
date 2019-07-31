package com.shf.flink.sample.batch.transformations;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/1 15:35
 */
public class JoinSample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<User> userDataSet = env.fromElements(new User("foo", 1),
                new User("bar", 2),
                new User("car", 3));
        DataSet<Store> storeDataSet = env.fromElements(new Store("store1", 2),
                new Store("store2", 1),
                new Store("store4", 4));
        // result dataset is typed as Tuple2
        JoinOperator.DefaultJoin<User, Store> result = userDataSet.join(storeDataSet)
                // key of the first input (users)
                .where("zip")
                // key of the second input (stores)
                .equalTo("zip");
        // (User{name='foo', zip=1},Store{name='store2', zip=1})
        // (User{name='bar', zip=2},Store{name='store1', zip=2})
        result.print();

        // (foo,store2,1)
        // (bar,store1,2)
        result.with(new JoinFunctionSample()).print();

        // (foo,store2,1)
        // (bar,store1,2)
        result.with(new FlatJoinFunctionSample()).print();

        // In order to guide the optimizer to pick the right execution strategy, use joinWithTiny or joinWithHuge
        // (User{name='foo', zip=1},Store{name='store2', zip=1})
        // (User{name='bar', zip=2},Store{name='store1', zip=2})
        userDataSet.joinWithTiny(storeDataSet)
                // key of the first input (users)
                .where("zip")
                // key of the second input (stores)
                .equalTo("zip").print();

        /*
          <pre>
               OPTIMIZER_CHOOSES: Equivalent to not giving a hint at all, leaves the choice to the system.

               BROADCAST_HASH_FIRST: Broadcasts the first input and builds a hash table from it, which is probed
               by the second input. A good strategy if the first input is very small.

               BROADCAST_HASH_SECOND: Broadcasts the second input and builds a hash table from it, which is probed
               by the first input. A good strategy if the second input is very small.

               REPARTITION_HASH_FIRST: The system partitions (shuffles) each input (unless the input is already partitioned)
               and builds a hash table from the first input. This strategy is good if the first input is smaller than the second,
               but both inputs are still large. Note: This is the default fallback strategy that the system uses if no size estimates can be made and no pre-existing partitions and sort-orders can be re-used.

               REPARTITION_HASH_SECOND: The system partitions (shuffles) each input (unless the input is already partitioned)
               and builds a hash table from the second input. This strategy is good if the second input is smaller than the first,
               but both inputs are still large.

               REPARTITION_SORT_MERGE: The system partitions (shuffles) each input (unless the input is already partitioned)
               and sorts each input (unless it is already sorted). The inputs are joined by a streamed merge of the sorted inputs.
               This strategy is good if one or both of the inputs are already sorted.
          </pre>
         */
        // (User{name='foo', zip=1},Store{name='store2', zip=1})
        // (User{name='bar', zip=2},Store{name='store1', zip=2})
        userDataSet.join(storeDataSet, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
                // key of the first input (users)
                .where("zip")
                // key of the second input (stores)
                .equalTo("zip").print();

        /****************************OUT JOIN**************************/
        // (car,null,3)
        // (foo,store2,1)
        // (bar,store1,2)
        userDataSet.leftOuterJoin(storeDataSet)
                // key of the first input (users)
                .where("zip")
                // key of the second input (stores)
                .equalTo("zip").with(new JoinFunctionSample()).print();

        // (foo,store2,1)
        // (bar,store1,2)
        // (null,store4,4)
        userDataSet.rightOuterJoin(storeDataSet)
                // key of the first input (users)
                .where("zip")
                // key of the second input (stores)
                .equalTo("zip").with(new JoinFunctionSample()).print();
    }

    public static class JoinFunctionSample
            implements JoinFunction<User, Store, Tuple3<String, String, Integer>> {

        @Override
        public Tuple3<String, String, Integer> join(User first, Store second) throws Exception {
            return Tuple3.of(null == first ? null : first.name,
                    null == second ? null : second.name,
                    null != first ? first.zip : null != second ? second.zip : null);
        }
    }

    public static class FlatJoinFunctionSample
            implements FlatJoinFunction<User, Store, Tuple3<String, String, Integer>> {
        @Override
        public void join(User first, Store second, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            out.collect(Tuple3.of(first.name, second.name, first.zip));
        }
    }

    public static class User {
        public String name;
        public int zip;

        public User() {
        }

        public User(String name, int zip) {
            this.name = name;
            this.zip = zip;
        }

        @Override
        public String toString() {
            return "User{" +
                    "name='" + name + '\'' +
                    ", zip=" + zip +
                    '}';
        }
    }

    public static class Store {
        public String name;
        public int zip;

        public Store() {
        }

        public Store(String name, int zip) {
            this.name = name;
            this.zip = zip;
        }

        @Override
        public String toString() {
            return "Store{" +
                    "name='" + name + '\'' +
                    ", zip=" + zip +
                    '}';
        }
    }
}
