package io.apicurio.registry.utils.converter.avro;

import io.debezium.time.Interval;
import io.debezium.time.IsoDate;
import io.debezium.time.MicroDuration;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class AvroDataTest {

    private static final String EXAMPLE_VALUE_NAME = "example_value";
    @Test
    public void testIntWithConnectDefault() {
        final String s = "{"
                + "  \"type\": \"record\","
                + "  \"name\": \"sample\","
                + "  \"namespace\": \"io.apicurio\","
                + "  \"fields\": ["
                + "    {"
                + "      \"name\": \"prop\","
                + "      \"type\": {"
                + "        \"type\": \"int\","
                + "        \"connect.default\": 42,"
                + "        \"connect.version\": 1"
                + "      }"
                + "    }"
                + "  ]"
                + "}";

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(s);

        AvroData avroData = new AvroData(0);
        Schema schema = avroData.toConnectSchema(avroSchema);

        Assertions.assertEquals(42, schema.field("prop").schema().defaultValue());
    }

    @Test
    public void testLongWithConnectDefault() {
        final String s = "{"
                + "  \"type\": \"record\","
                + "  \"name\": \"sample\","
                + "  \"namespace\": \"io.apicurio\","
                + "  \"fields\": ["
                + "    {"
                + "      \"name\": \"prop\","
                + "      \"type\": {"
                + "        \"type\": \"long\","
                + "        \"connect.default\": 42,"
                + "        \"connect.version\": 1"
                + "      }"
                + "    }"
                + "  ]"
                + "}";

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(s);

        AvroData avroData = new AvroData(0);
        Schema schema = avroData.toConnectSchema(avroSchema);

        Assertions.assertEquals(42L, schema.field("prop").schema().defaultValue());
    }

    @Test
    public void testAvroInt64WithInteger() {
        final String s = "{"
                + "  \"type\": \"record\","
                + "  \"name\": \"sample\","
                + "  \"namespace\": \"io.apicurio\","
                + "  \"fields\": ["
                + "    {"
                + "      \"name\": \"someprop\","
                + "      \"type\": [\"long\",\"null\"]"
                + "    }"
                + "  ]"
                + "}";

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(s);
        GenericRecord outputRecord = new GenericRecordBuilder(avroSchema).set("someprop", (long) 42).build();
        AvroData avroData = new AvroData(0);
        Assertions.assertDoesNotThrow(() -> avroData.toConnectData(avroSchema, outputRecord));
    }

    @Test
    public void testDecimal() {
        final String s = "{"
                + "  \"type\": \"record\","
                + "  \"name\": \"sample\","
                + "  \"namespace\": \"io.apicurio\","
                + "  \"fields\": ["
                + "    {"
                + "      \"name\": \"somedecimal\","
                + "      \"type\": [\n"
                + "          {\n"
                + "            \"type\": \"bytes\",\n"
                + "            \"scale\": 4,\n"
                + "            \"precision\": 4,\n"
                + "            \"connect.version\": 1,\n"
                + "            \"connect.parameters\": {\n"
                + "              \"scale\": \"4\",\n"
                + "              \"connect.decimal.precision\": \"4\"\n"
                + "            },\n"
                + "            \"connect.default\": \"AA==\",\n"
                + "            \"connect.name\": \"org.apache.kafka.connect.data.Decimal\",\n"
                + "            \"logicalType\": \"decimal\"\n"
                + "          },\n"
                + "          \"null\"\n"
                + "       ],\n"
                + "       \"default\": \"AA==\""
                + "    }"
                + "  ],"
                + "\"connect.name\":\"io.apicurio.sample\"\n"
                + "}";

        org.apache.avro.Schema bSchema = new org.apache.avro.Schema.Parser().parse(s);
        AvroData avroData = new AvroData(0);
        org.apache.avro.Schema aSchema = avroData.fromConnectSchema(avroData.toConnectSchema(bSchema));
        Assertions.assertEquals(bSchema.toString(), aSchema.toString());
    }

    @Test
    void testCacheDistinguishesByParameters() {
        AvroData avroData = new AvroData(5);
        // Create two Connect schemas with the same name but different parameters
        Schema schemaWithPrecision10 = createConnectSchema("io.debezium.data.VariableScaleDecimal", "10");
        Schema schemaWithPrecision15 = createConnectSchema("io.debezium.data.VariableScaleDecimal", "15");

        // Generate Avro schemas for both
        org.apache.avro.Schema avroSchema1 = avroData.fromConnectSchema(schemaWithPrecision10);
        org.apache.avro.Schema avroSchema2 = avroData.fromConnectSchema(schemaWithPrecision15);

        // Verify that the two schemas are different
        assertNotEquals(avroSchema1, avroSchema2, "Avro schemas with different parameters should not be equal");

        // Verify that repeated calls with the same schema return the same instance (cache hit)
        org.apache.avro.Schema avroSchema1Again = avroData.fromConnectSchema(schemaWithPrecision10);
        assertSame(avroSchema1, avroSchema1Again, "Repeated calls with the same schema should return the cached instance");

        org.apache.avro.Schema avroSchema2Again = avroData.fromConnectSchema(schemaWithPrecision15);
        assertSame(avroSchema2, avroSchema2Again, "Repeated calls with the same schema should return the cached instance");
    }

    private Schema createConnectSchema(String name, String precision) {
        // Create a struct schema similar to the example provided
        return SchemaBuilder.struct()
                .name(name)
                .version(1)
                .doc("Variable scaled decimal")
                .field("scale", Schema.INT32_SCHEMA)
                .field("value", Schema.BYTES_SCHEMA)
                .parameter("precision", precision)
                .optional()
                .build();
    }

    @Test
    public void testDebeziumTimestampType() {
        long debeziumTimestamp = io.debezium.time.Timestamp.toEpochMillis(new Date(), null);
        String primitiveAvroType = "long";
        org.apache.avro.Schema expectedAvroSchema = this.expectedAvroTypeSchema(io.debezium.time.Timestamp.SCHEMA_NAME, primitiveAvroType, "timestamp-millis");

        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(io.debezium.time.Timestamp.SCHEMA_NAME, primitiveAvroType, debeziumTimestamp);
        Assertions.assertEquals(debeziumTimestamp, ((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Long.class, expectedAvroSchema));
        Assertions.assertEquals(debeziumTimestamp, outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumDateType() {
        int epochDays = io.debezium.time.Date.toEpochDay(new Date(), null);
        String primitiveAvroType = "int";

        org.apache.avro.Schema expectedTypeSchema = this.expectedAvroTypeSchema(io.debezium.time.Date.SCHEMA_NAME, primitiveAvroType, "date");
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(io.debezium.time.Date.SCHEMA_NAME, primitiveAvroType, epochDays);
        Assertions.assertEquals(epochDays, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Integer.class, expectedTypeSchema));
        Assertions.assertEquals(epochDays, outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumIntervalType() {
        String interval = Interval.toIsoString(1, 2, 3, 4, 5, BigDecimal.valueOf(6));

        int leMonths = Integer.reverseBytes(14);
        int leDays = Integer.reverseBytes(3);
        int leMillis = Integer.reverseBytes(4*60*60*1_000 + 5*60*1_000 + 6*1_000);

        byte[] fixedInterval = ByteBuffer.allocate(12).putInt(leMonths).putInt(leDays).putInt(leMillis).array();

        String expectedAvroSchemaString =
                "{" +
                        "  \"type\" : \"fixed\"," +
                        "  \"name\" : \"debeziumDuration\"," +
                        "  \"size\" : 12," +
                        "  \"connect.version\" : 1," +
                        "  \"connect.name\" : \"" + Interval.SCHEMA_NAME + "\"," +
                        "  \"logicalType\" : \"duration\"" +
                        "}";

        org.apache.avro.Schema expectedTypeSchema = new org.apache.avro.Schema.Parser().parse(expectedAvroSchemaString);
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(Interval.SCHEMA_NAME, "string", interval);
        Assertions.assertEquals(interval, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(outputRecord.getSchema().getField(EXAMPLE_VALUE_NAME).schema().getTypes().contains(expectedTypeSchema));
        Assertions.assertArrayEquals(fixedInterval, (byte[]) outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumIsoDateType() {
        // ZonedDateTime.format with DateTimeFormatter.ISO_DATE or ISO_OFFSET_DATE returns the ISO date with a Z appended (e.g. "2025-01-01Z")
        // String isoDate = io.debezium.time.IsoDate.toIsoString(LocalDate.ofYearDay(2025, 1), null);
        String isoDate = "2025-01-01";
        int daysSinceEpoch = (int) LocalDate.parse(isoDate, DateTimeFormatter.ISO_DATE).toEpochDay();
        String primitiveAvroType = "int";

        org.apache.avro.Schema expectedTypeSchema = this.expectedAvroTypeSchema(IsoDate.SCHEMA_NAME, primitiveAvroType, "date");
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(IsoDate.SCHEMA_NAME, "string", isoDate);
        Assertions.assertEquals(isoDate, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Integer.class, expectedTypeSchema));
        Assertions.assertEquals(daysSinceEpoch, outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumIsoTimeType() {
        String isoTime = io.debezium.time.IsoTime.toIsoString(LocalTime.of(13, 37, 42), true);
        int millisSinceMidnight = 60*60*1_000*13 + 60*1_000*37 + 1_000*42;
        String primitiveAvroType = "int";

        org.apache.avro.Schema expectedTypeSchema = this.expectedAvroTypeSchema(io.debezium.time.IsoTime.SCHEMA_NAME, primitiveAvroType, "time-millis");
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(io.debezium.time.IsoTime.SCHEMA_NAME, "string", isoTime);
        Assertions.assertEquals(isoTime, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Integer.class, expectedTypeSchema));
        Assertions.assertEquals(millisSinceMidnight, outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumIsoTimestampType() {

        String isoTimestamp = io.debezium.time.IsoTimestamp.toIsoString(LocalDateTime.of(2011, 12, 3, 10, 15, 30), null);
        long epochMilli = Instant.parse(isoTimestamp).toEpochMilli();
        String primitiveAvroType = "long";

        org.apache.avro.Schema expectedTypeSchema = this.expectedAvroTypeSchema(io.debezium.time.IsoTimestamp.SCHEMA_NAME, primitiveAvroType, "timestamp-millis");
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(io.debezium.time.IsoTimestamp.SCHEMA_NAME, "string", isoTimestamp);
        Assertions.assertEquals(isoTimestamp, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Long.class, expectedTypeSchema));
        Assertions.assertEquals(epochMilli, outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumMicroDurationType() {
        long durationMicros = MicroDuration.durationMicros(0, 0, 3, 4, 5, 6, null);

        int leDays = Integer.reverseBytes(3);
        int leMillis = Integer.reverseBytes(4*60*60*1_000 + 5*60*1_000 + 6*1_000);

        byte[] fixedDuration = ByteBuffer.allocate(12).putInt(0).putInt(leDays).putInt(leMillis).array();

        String expectedAvroSchemaString =
                "{" +
                        "  \"type\" : \"fixed\"," +
                        "  \"name\" : \"debeziumDuration\"," +
                        "  \"size\" : 12," +
                        "  \"connect.version\" : 1," +
                        "  \"connect.name\" : \"" + MicroDuration.SCHEMA_NAME + "\"," +
                        "  \"logicalType\" : \"duration\"" +
                        "}";

        org.apache.avro.Schema expectedTypeSchema = new org.apache.avro.Schema.Parser().parse(expectedAvroSchemaString);
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(MicroDuration.SCHEMA_NAME, "long", durationMicros);
        Assertions.assertEquals(durationMicros, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(outputRecord.getSchema().getField(EXAMPLE_VALUE_NAME).schema().getTypes().contains(expectedTypeSchema));
        Assertions.assertArrayEquals(fixedDuration, (byte[]) outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumMicroTimeType() {
        long microTime = io.debezium.time.MicroTime.toMicroOfDay(Duration.ofMinutes(5), true);
        long microsSinceMidnight = 5*60*1_000*1_000;
        String primitiveAvroType = "long";

        org.apache.avro.Schema expectedTypeSchema = this.expectedAvroTypeSchema(io.debezium.time.MicroTime.SCHEMA_NAME, primitiveAvroType, "time-micros");
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(io.debezium.time.MicroTime.SCHEMA_NAME, primitiveAvroType, microTime);
        Assertions.assertEquals(microTime, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Long.class, expectedTypeSchema));
        Assertions.assertEquals(microsSinceMidnight, outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumMicroTimestampType() {
        Date now = new Date();
        long microTimestamp = io.debezium.time.MicroTimestamp.toEpochMicros(now, null);
        String primitiveAvroType = "long";

        org.apache.avro.Schema expectedTypeSchema = this.expectedAvroTypeSchema(io.debezium.time.MicroTimestamp.SCHEMA_NAME, primitiveAvroType, "timestamp-micros");
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(io.debezium.time.MicroTimestamp.SCHEMA_NAME, primitiveAvroType, microTimestamp);
        Assertions.assertEquals(microTimestamp, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Long.class, expectedTypeSchema));
        Assertions.assertEquals(microTimestamp, outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumTimeType() {
        int milliTime = io.debezium.time.Time.toMilliOfDay(Duration.ofMinutes(5), true);
        int millisSinceMidnight = 5*60*1_000;
        String primitiveAvroType = "int";

        org.apache.avro.Schema expectedTypeSchema = this.expectedAvroTypeSchema(io.debezium.time.Time.SCHEMA_NAME, primitiveAvroType, "time-millis");
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(io.debezium.time.Time.SCHEMA_NAME, primitiveAvroType, milliTime);
        Assertions.assertEquals(milliTime, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Integer.class, expectedTypeSchema));
        Assertions.assertEquals(millisSinceMidnight, outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumZonedTimeType() {

        String zonedTimeIsoString = io.debezium.time.ZonedTime.toIsoString(OffsetTime.of(5, 41, 4, 317_000_000, ZoneOffset.ofHours(2)), ZoneId.of("Europe/Rome"), null);
        int millisSinceMidnight = (int) (LocalTime.parse(zonedTimeIsoString, DateTimeFormatter.ISO_TIME).toNanoOfDay() / 1_000_000);
        String primitiveAvroType = "int";

        org.apache.avro.Schema expectedTypeSchema = this.expectedAvroTypeSchema(io.debezium.time.ZonedTime.SCHEMA_NAME, primitiveAvroType, "time-millis");
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(io.debezium.time.ZonedTime.SCHEMA_NAME, "string", zonedTimeIsoString);
        Assertions.assertEquals(zonedTimeIsoString, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Integer.class, expectedTypeSchema));
        Assertions.assertEquals(millisSinceMidnight, outputRecord.get(EXAMPLE_VALUE_NAME));
    }

    @Test
    public void testDebeziumZonedTimestampType() {

        String zonedTimestampIsoString = io.debezium.time.ZonedTimestamp.toIsoString(
                OffsetDateTime.of(LocalDateTime.of(2011, 12, 3, 8, 15, 30), ZoneOffset.ofHours(2)),
                ZoneId.of("Europe/Rome"),
                null,
                null);
        long unixTimestamp = Instant.parse(zonedTimestampIsoString).toEpochMilli();
        String primitiveAvroType = "long";

        org.apache.avro.Schema expectedTypeSchema = this.expectedAvroTypeSchema(io.debezium.time.ZonedTimestamp.SCHEMA_NAME, primitiveAvroType, "timestamp-millis");
        SchemaAndValue outputSchemaValue = this.debeziumAvroToConnect(io.debezium.time.ZonedTimestamp.SCHEMA_NAME, "string", zonedTimestampIsoString);
        Assertions.assertEquals(zonedTimestampIsoString, (((Struct) outputSchemaValue.value()).get(EXAMPLE_VALUE_NAME)));

        GenericRecord outputRecord = this.genericRecordFromConnect(outputSchemaValue);
        Assertions.assertTrue(this.isRecordValid(outputRecord, Long.class, expectedTypeSchema));
        Assertions.assertEquals(unixTimestamp, outputRecord.get(EXAMPLE_VALUE_NAME));
    }


    private org.apache.avro.Schema expectedAvroTypeSchema(String connectName, String primitiveType, String logicalType) {
        String typeSchemaString =
                "{" +
                        "  \"type\" : \"" + primitiveType + "\"," +
                        "  \"connect.version\" : 1," +
                        "  \"connect.name\" : \"" + connectName + "\"," +
                        "  \"logicalType\" : \"" + logicalType + "\"" +
                        "}";
        return new org.apache.avro.Schema.Parser().parse(typeSchemaString);
    }

    private <T> SchemaAndValue debeziumAvroToConnect(String connectName, String avroType, T debeziumValue){
        final String AVROSCHEMA = "{\"type\": \"record\", " +
                "\"name\": \"debezium_test\", " +
                "\"connect.name\": \"io.apicurio.sample\", " +
                "\"fields\": [" +
                "   {\"type\": " +
                "       [ " +
                "           \"null\", " +
                "           {\"type\": \"" + avroType + "\", \"connect.version\": 1, \"connect.name\": \"" + connectName + "\"}" +
                "       ]," +
                "      \"name\": \"" + EXAMPLE_VALUE_NAME + "\"" +
                "   }" +
                "]}";

        AvroData avroData = new AvroData(0);
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(AVROSCHEMA);
        GenericRecord record = new GenericRecordBuilder(avroSchema).set(EXAMPLE_VALUE_NAME, debeziumValue).build();
        return avroData.toConnectData(avroSchema, record);
    }

    private GenericRecord genericRecordFromConnect(SchemaAndValue connectSchemaValue){
        AvroData avroData = new AvroData(0);
        return (GenericRecord) avroData.fromConnectData(connectSchemaValue.schema(), connectSchemaValue.value());
    }

    private boolean isRecordValid(GenericRecord record, Class<?> expectedType, org.apache.avro.Schema expectedAvroSchema) {
        return expectedType.isInstance(record.get(EXAMPLE_VALUE_NAME))
                && record.getSchema().getField(EXAMPLE_VALUE_NAME).schema().getTypes().contains(expectedAvroSchema);
    }
}
