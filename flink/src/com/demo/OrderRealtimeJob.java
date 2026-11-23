package com.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 电商订单实时分析作业（Flink）
 *
 * 数据流：Kafka topic `orders` -> Flink（事件时间 1 分钟滚动窗口）-> Kafka topic `orders_flink_agg`
 * 指标：按品类统计每分钟的订单笔数、GMV（已支付金额合计）、客单价、取消笔数
 * 说明：event time 用订单自带的 order_time（带 +08:00 时区），水印容忍 5 秒乱序
 */
public class OrderRealtimeJob {

    private static final ZoneId CST = ZoneId.of("Asia/Shanghai");
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** 订单事件（POJO，Flink 可走 Kryo/Pojo 序列化） */
    public static class Order {
        public String orderId;
        public String userId;
        public String productId;
        public String category;
        public String status;
        public double price;
        public int quantity;
        public double amount;
        public long ts;
        public Order() {}
    }

    public static void main(String[] args) throws Exception {
        final String brokers = getEnv("KAFKA_BOOTSTRAP", "kafka:9092");
        final String srcTopic = getEnv("SRC_TOPIC", "orders");
        final String sinkTopic = getEnv("SINK_TOPIC", "orders_flink_agg");
        final String groupId = getEnv("GROUP_ID", "flink-orders-realtime");
        final String parallelism = getEnv("PARALLELISM", "2");
        final String windowMin = getEnv("WINDOW_MINUTES", "1");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(parallelism));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(srcTopic)
                .setGroupId(groupId)
                // 从最早位点起读：作业重启后重放历史订单，保证演示时一定能看到聚合结果
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        final ObjectMapper mapper = new ObjectMapper();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-orders-source");

        DataStream<Order> orders = raw
                .map(line -> parse(line, mapper))
                .filter(o -> o != null)
                .name("parse-json");

        DataStream<Order> withWatermark = orders.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((o, t) -> o.ts)
                        .withIdleness(Duration.ofSeconds(30))
        );

        DataStream<String> agg = withWatermark
                .keyBy(o -> o.category)
                .window(TumblingEventTimeWindows.of(Time.minutes(Integer.parseInt(windowMin))))
                .process(new CategoryWindowAgg())
                .name("window-aggregate");

        // 1) 打到 TaskManager 日志，方便 docker logs 直接验收
        agg.print("FLINK_AGG").name("print-sink");

        // 2) 写入 Kafka，供 Kibana / 后续链路消费
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(sinkTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
        agg.sinkTo(sink).name("kafka-agg-sink");

        env.execute("ecommerce-orders-realtime-analysis");
    }

    private static String getEnv(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }

    private static Order parse(String line, ObjectMapper m) {
        try {
            JsonNode n = m.readTree(line);
            Order o = new Order();
            o.orderId = n.path("order_id").asText();
            o.userId = n.path("user_id").asText();
            o.productId = n.path("product_id").asText();
            o.category = n.path("category").asText("unknown");
            o.status = n.path("status").asText("unknown");
            o.price = n.path("price").asDouble(0);
            o.quantity = n.path("quantity").asInt(0);
            o.amount = n.path("amount").asDouble(0);
            String t = n.path("order_time").asText();
            o.ts = OffsetDateTime.parse(t).toInstant().toEpochMilli();
            return o;
        } catch (Exception e) {
            return null;   // 脏数据直接丢弃，不阻断流
        }
    }

    /** 按品类 + 窗口聚合 */
    public static class CategoryWindowAgg
            extends ProcessWindowFunction<Order, String, String, TimeWindow> {

        @Override
        public void process(String category, Context ctx, Iterable<Order> elements, Collector<String> out) {
            long cnt = 0, paidCnt = 0, cancelCnt = 0;
            double gmv = 0.0, totalAmount = 0.0;
            for (Order o : elements) {
                cnt++;
                totalAmount += o.amount;
                if ("paid".equalsIgnoreCase(o.status)) {
                    paidCnt++;
                    gmv += o.amount;
                } else if ("cancelled".equalsIgnoreCase(o.status)) {
                    cancelCnt++;
                }
            }
            double avg = cnt == 0 ? 0.0 : round2(totalAmount / cnt);
            String ws = fmt(ctx.window().getStart());
            String we = fmt(ctx.window().getEnd());
            String json = String.format(
                    "{\"window_start\":\"%s\",\"window_end\":\"%s\",\"category\":\"%s\"," +
                    "\"order_cnt\":%d,\"paid_cnt\":%d,\"cancel_cnt\":%d," +
                    "\"gmv\":%.2f,\"avg_amount\":%.2f}",
                    ws, we, category, cnt, paidCnt, cancelCnt, round2(gmv), avg);
            out.collect(json);
        }

        private static String fmt(long epochMillis) {
            return Instant.ofEpochMilli(epochMillis).atZone(CST).format(FMT);
        }

        private static double round2(double v) {
            return Math.round(v * 100.0) / 100.0;
        }
    }
}
