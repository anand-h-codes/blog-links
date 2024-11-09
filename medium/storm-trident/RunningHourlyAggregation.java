package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.TransactionalMap;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class RunningHourlyAggregation {

    static final long ONE_HOUR = 60 * 60 * 1000;

    public static void main(String[] args) throws Exception {

        long now = System.currentTimeMillis();

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("key", "timestamp", "value"), 3,
                new Values("sensor1", now - ONE_HOUR, 100L),
                new Values("sensor2", now - ONE_HOUR, 200L),
                new Values("sensor3", now - ONE_HOUR, 300L),
                new Values("sensor4", now - ONE_HOUR, 400L),
                new Values("sensor1", now - ONE_HOUR/2, 500L),
                new Values("sensor2", now - ONE_HOUR/2, 600L),
                new Values("sensor3", now - ONE_HOUR/2, 700L),
                new Values("sensor4", now - ONE_HOUR/2, 800L),
                new Values("sensor1", now, 900L),
                new Values("sensor2", now, 1000L),
                new Values("sensor3", now, 1100L),
                new Values("sensor4", now, 1200L)
        );

        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newStream("spout", spout)
                .each(new Fields("key", "timestamp", "value"),
                        new CustomTimestampSplitFunction(),
                        new Fields("year","month","day","hour","minute","seconds"))
                .groupBy(new Fields("key", "year","month","day","hour"))
                .persistentAggregate(new CustomMySQLStateFactory(),
                        new Fields("key","year","month","day","hour","value"),
                        new CustomCombinerAggregator(),
                        new Fields("agg"));

        StormTopology topology = tridentTopology.build();
        new LocalCluster().submitTopology("agg-topology", new Config(), topology);

    }

    private static class CustomTimestampSplitFunction implements Function {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Long timestamp = tuple.getLongByField("timestamp");
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timestamp);
            collector.emit(new Values(
                    calendar.get(Calendar.YEAR),
                    calendar.get(Calendar.MONTH),
                    calendar.get(Calendar.DAY_OF_MONTH),
                    calendar.get(Calendar.HOUR_OF_DAY),
                    calendar.get(Calendar.MINUTE),
                    calendar.get(Calendar.SECOND),
                    tuple.getLongByField("value")));
        }

        @Override
        public void prepare(Map<String, Object> conf, TridentOperationContext context) {
        }

        @Override
        public void cleanup() {
        }
    }

    private static class CustomMySQLStateFactory implements StateFactory {
        @Override
        public State makeState(Map<String, Object> conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            return TransactionalMap.build(new IBackingMap<>() {
                @Override
                public List<TransactionalValue> multiGet(List<List<Object>> keys) {
                    List<AggregatedRecord> recordsToQuery = new ArrayList<>();
                    for (List<Object> key : keys) {
                        recordsToQuery.add(new AggregatedRecord((String) key.get(0),
                                (Integer) key.get(1), (Integer) key.get(2),
                                (Integer) key.get(3), (Integer) key.get(3),
                                0, 0, -1, true));
                    }
                    return getRecords(recordsToQuery);
                }

                @Override
                public void multiPut(List<List<Object>> keys, List<TransactionalValue> vals) {
                    updateRecords(vals);
                }
            });
        }
        private static Connection getConnection(){
            Connection connection = null;
            try {
                String url = "jdbc:mysql://localhost:3306/dbname";
                String user = "username";
                String password = "password";
                connection = DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return connection;
        }

        private static List<TransactionalValue> getRecords(List<AggregatedRecord> recordsToQuery) {

            Map<String, AggregatedRecord> queryMap = new LinkedHashMap<>();
            Map<String, TransactionalValue> resultMap = new LinkedHashMap<>();
            recordsToQuery.forEach(r -> queryMap.put(r.buildKey(), r));
            try{
                ResultSet resultSet;
                try (Statement statement = getConnection().createStatement()) {
                    StringBuilder sql = new StringBuilder("select sensor, year, month, day, hour, sum, count, txid from Aggregated_Data where ");
                    for (AggregatedRecord r : recordsToQuery) {
                        sql.append(" (sensor= '").append(r.sensor).append("' and year = ").append(r.year)
                                .append(" and month = ").append(r.month)
                                .append(" and day = ").append(r.day)
                                .append(" and hour = ").append(r.hour)
                                .append(") ");
                        if (recordsToQuery.indexOf(r) != recordsToQuery.size() - 1) {
                            sql.append(" or ");
                        }
                    }
                    resultSet = statement.executeQuery(sql.toString());
                    while(resultSet.next()){
                        AggregatedRecord aggregatedRecord = new AggregatedRecord(resultSet.getString("sensor"),
                                resultSet.getInt("year"),
                                resultSet.getInt("month"),
                                resultSet.getInt("day"),
                                resultSet.getInt("hour"),
                                resultSet.getLong("sum"),
                                resultSet.getLong("count"),
                                resultSet.getLong("txid"),
                                false);
                        resultMap.put(aggregatedRecord.buildKey(), new TransactionalValue<>(resultSet.getLong("txid"), aggregatedRecord));
                    }
                }
                List<TransactionalValue> ret = new ArrayList<>();
                queryMap.forEach((k,v) -> {
                    if(resultMap.containsKey(k)) {
                        ret.add(resultMap.get(k));
                    }else{
                        ret.add(new TransactionalValue(null, v));
                    }
                });
                return ret;
            }catch (SQLException e){
                throw new RuntimeException(e);
            }
        }

        private static void updateRecords(List<TransactionalValue> vals) {
            try{
                try (Statement statement = getConnection().createStatement()) {
                    List<String> updateSqls = new ArrayList<>();
                    StringBuilder insertSql = new StringBuilder("insert into Aggregated_Data (sensor, year, month, day, hour, sum, count, txid) values ");
                    boolean insertFlag = false;
                    for (int i = 0; i < vals.size(); i++) {
                        AggregatedRecord record = (AggregatedRecord) vals.get(i).getVal();
                        if(record.isNew){
                            insertSql.append("('").append(record.sensor).append("', ")
                                    .append(record.year).append(", ")
                                    .append(record.month).append(", ")
                                    .append(record.day).append(", ")
                                    .append(record.hour).append(", ")
                                    .append(record.sum).append(", ")
                                    .append(record.count).append(", ")
                                    .append(vals.get(i).getTxid()).append(")");
                            if(i != vals.size() - 1){
                                insertSql.append(",");
                            }
                            insertFlag = true;
                        }else{
                            updateSqls.add("update Aggregated_Data set " +
                                    " sum = " + record.sum + ", " +
                                    " count = " + record.count + ", " +
                                    " txid = " + vals.get(i).getTxid() +
                                    " where sensor = '" + record.sensor + "'" +
                                    " and year = " + record.year +
                                    " and month = " + record.month +
                                    " and day = " + record.day +
                                    " and hour = " + record.hour +
                                    ";");
                        }
                    }
                    if(insertFlag)
                        statement.executeUpdate(insertSql.toString());

                    for (String updateSql : updateSqls) {
                        statement.executeUpdate(updateSql);
                    }

                }
            }catch (SQLException e){
                throw new RuntimeException(e);
            }
        }
    }

    private static class CustomCombinerAggregator implements CombinerAggregator<AggregatedRecord> {
        @Override
        public AggregatedRecord init(TridentTuple tuple) {
            return new AggregatedRecord(tuple.getStringByField("key"),
                    tuple.getIntegerByField("year"), tuple.getIntegerByField("month"),
                    tuple.getIntegerByField("day"), tuple.getIntegerByField("hour"),
                    tuple.getLongByField("value"), 1, 0, true);
        }

        @Override
        public AggregatedRecord combine(AggregatedRecord val1, AggregatedRecord val2) {
            return new AggregatedRecord(val1.sensor, val1.year,val1.month,val1.day,val1.hour,
                    val1.sum + val2.sum, val1.count + val2.count,
                    val2.txid, val1.isNew);
        }

        @Override
        public AggregatedRecord zero() {
            return null;
        }
    }

    private static class AggregatedRecord {
        public String sensor;
        public int year;
        public int month;
        public int day;
        public int hour;

        private long sum;
        private long count;
        private long txid;
        public boolean isNew;

        public AggregatedRecord(String sensor, int year, int month, int day, int hour, long sum, long count, long txid, boolean isNew) {
            this.sensor = sensor;
            this.year = year;
            this.month = month;
            this.day = day;
            this.hour = hour;
            this.sum = sum;
            this.count = count;
            this.txid = txid;
            this.isNew = isNew;
        }

        public String buildKey() {
            return sensor + year + month + day + hour;
        }
    }

}
