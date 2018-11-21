package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.Rowtime;

import java.util.Properties;
import java.util.UUID;

public class ReadWriteKafkaTableSQLAPI {
    public static void main(String[] args) throws Exception {
        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        if(params.getNumberOfParameters() < 4) {
            System.out.println("\nUsage: ReadWriteKafkaTableSQLAPI --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
            return;
        }

        Properties kparams = params.getProperties();
        kparams.setProperty("auto.offset.reset", "earliest");
        kparams.setProperty("flink.starting-position", "earliest");
        kparams.setProperty("group.id", UUID.randomUUID().toString());

        // setup streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic(params.getRequired("read-topic"))
                .property("bootstrap.servers", params.getRequired("bootstrap.servers")))
                .withSchema(new Schema()
                        .field("sensor", Types.STRING())
                        .field("temp", Types.LONG())
                        .field("ts", Types.SQL_TIMESTAMP())
                        .rowtime(new Rowtime()
                                .timestampsFromSource()
                                .watermarksPeriodicBounded(1000)
                        )
                )
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                .registerTableSource("sourceTopic");

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic(params.getRequired("write-topic"))
                .property("bootstrap.servers", params.getRequired("bootstrap.servers"))
                .sinkPartitionerRoundRobin())
                .withSchema(new Schema()
                        .field("sensor", Types.STRING())
                        .field("tumbleStart", Types.SQL_TIMESTAMP())
                        .field("tumbleEnd", Types.SQL_TIMESTAMP())
                        .field("avgTemp", Types.LONG())
                )
                .withFormat(new Json().deriveSchema())
                .inAppendMode()
                .registerTableSink("sinkTopic");

        String sql = "INSERT INTO sinkTopic "
                + "SELECT sensor, "
                + "TUMBLE_START(ts, INTERVAL '1' MINUTE) as tumbleStart, "
                + "TUMBLE_END(ts, INTERVAL '1' MINUTE) as tumbleEnd, "
                + "AVG(temp) AS avgTemp "
                + "FROM sourceTopic "
                + "WHERE sensor IS NOT null "
                + "GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), sensor";

        tableEnv.sqlUpdate(sql);

        env.execute("ReadWriteKafkaTableSQLAPI");
    }

}

