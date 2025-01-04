/*
 * Copyright (c) 2023, 2025 James C. McPherson. All rights reserved.
 *
 * This work is covered by the Hippocratic License v3, as documented
 * in [LICENSE.md](LICENSE.md)
 */

package com.jmcpdotcom.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import java.io.InputStream;
import java.io.IOException;
import java.lang.StringBuilder;
import java.util.*;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;

import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

public class UnwrapperTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Logger testLogger = (Logger) LoggerFactory.getLogger(UnwrapperTest.class);

    private Unwrapper<SinkRecord> createRouter(String logLevel) {
        final Map<String, String> props = new HashMap<>();
        props.put(Unwrapper.ConfigName.LOGLEVEL, Level.toLevel(logLevel, Level.DEBUG).toString());
        final Unwrapper<SinkRecord> router = new Unwrapper<>();
        router.configure(props);
        return router;
    }

    private Object apply(String valueString, String logLevel) {
        Unwrapper<SinkRecord> router = createRouter(logLevel);
        ConnectRecord<SinkRecord> rRec = router.apply(new SinkRecord("test topic", 0,
                null, null,
                null, valueString, 0));
        String rval = rRec.value().toString();
        router.close();
        return rval;
    }

    private Object apply(SchemaAndValue inputSAV, String logLevel) {
        Unwrapper<SinkRecord> router = createRouter(logLevel);
        ConnectRecord<SinkRecord> rRec = router.apply(new SinkRecord("test topic", 0,
                null, null,
                inputSAV.schema(), inputSAV.value(), 0));
        String rval = rRec.value().toString();
        router.close();
        return rval;
    }

    private StringBuilder schemaPrettyPrint(Schema sav, int indent) {
        /*
         * Create a pretty-printed StringBuilder which contains the Schema as
         * data type and field name. It's a bit YAML-like - STRUCT elements are
         * indented another level (2 chars), while non-STRUCTs are not. We don't
         * include the value each field contains.
         */
        List<Field> genfields = sav.fields();
        String indentstr = "  ".repeat(indent);
        StringBuilder rstr = new StringBuilder();

        for (Field gf: genfields) {
            if (gf.schema().type() == Schema.Type.ARRAY || gf.schema().type() == Schema.Type.STRUCT) {
                rstr.append(indentstr).append(gf.name()).append(" :\n");
                rstr.append(schemaPrettyPrint(gf.schema(), (indent + 1)));
            } else {
                rstr.append(indentstr)
                        .append(String.format("%1$-10s %2$-30s\n",
                                        gf.schema().type().toString(), gf.name()));
            }
        }
        return rstr;
    }

    private SchemaAndValue generateRecordFromJson(JsonNode treeToRead, String schemaName, boolean isArray) {

        testLogger.atDebug().log("generateRecordFromJson(''{}'', ''{}'', {})\n",
                treeToRead, schemaName, isArray);

        SchemaBuilder toBuild;
        Map<String, Object> values = new HashMap<>();
        int idx = 0;

        if (treeToRead.getNodeType() != JsonNodeType.OBJECT && treeToRead.getNodeType() != JsonNodeType.ARRAY) {
            /* A *plain* element - just add it */
            toBuild = SchemaBuilder.type(Unwrapper.jsonNodeTypeToConnect(treeToRead.getNodeType(),
                    treeToRead).type()).name(schemaName);
            values.put(schemaName, Unwrapper.jsonValueToConnect(treeToRead));
        } else {
            toBuild = SchemaBuilder.struct().name(schemaName);
            for (Iterator<Map.Entry<String, JsonNode>> nodeFields = treeToRead.fields(); nodeFields.hasNext(); ) {

                Map.Entry<String, JsonNode> theNode = nodeFields.next();
                JsonNodeType theType = theNode.getValue().getNodeType();

                String nodeName = (isArray) ? theNode.getKey().concat("_") + idx : theNode.getKey();

                testLogger.atTrace().log("nodeType '{}', node contents {}", theType, theNode.getValue());

                switch (theType) {
                    case OBJECT:
                        /* recurse downwards */
                        SchemaAndValue childSAV = generateRecordFromJson(theNode.getValue(), nodeName, isArray);
                        values.put(nodeName, childSAV.value());
                        toBuild.field(nodeName, childSAV.schema());
                        ++idx;
                        break;

                    case ARRAY:
                        int innerIdx = 0;
                        String innerNodeName;

                        for (JsonNode childNode : theNode.getValue()) {

                            innerNodeName = theNode.getKey().concat("_") + innerIdx;
                            /* If there's an embedded OBJECT we have to go deeper */
                            JsonNodeType childType = childNode.getNodeType();

                            if (childType == JsonNodeType.OBJECT) {
                                SchemaAndValue objSAV = generateRecordFromJson(childNode, innerNodeName, true);
                                values.put(innerNodeName, objSAV.value());
                                toBuild.field(innerNodeName, objSAV.schema());
                            } else {
                                Schema arraySchema = Unwrapper.jsonNodeTypeToConnect(childType, childNode);
                                values.put(innerNodeName, Unwrapper.jsonValueToConnect(childNode));
                                toBuild.field(innerNodeName, arraySchema.schema());
                            }
                            ++innerIdx;
                        }
                        break;

                    default:
                        values.put(theNode.getKey(), Unwrapper.jsonValueToConnect(theNode.getValue()));
                        toBuild.field(theNode.getKey(), Unwrapper.jsonNodeTypeToConnect(theType, theNode.getValue()));
                        break;
                }
            }
        }
        testLogger.atDebug().log("\nvalues: {}", values);
        return Unwrapper.addDataToStruct(toBuild.build(), values);
    }

    /*
     * Common method to open, read and apply() a file. Returns a List<String>, where
     * the first element is the direct-from-file conversion and the second is the
     * from-file-through-JSON conversion
     */
    private List<String> infileToStrings(String infile, String loglevel, boolean prettyPrint) {
        List<String> rval = new ArrayList<>();

        try (InputStream inStream = UnwrapperTest.class.getResourceAsStream(infile)) {

            String exampleInput = null;
            JsonNode topNode;
            SchemaAndValue exampleSAV = null;

            try {
                exampleInput = new String(inStream.readAllBytes());
            } catch (IOException | NullPointerException ioe) {
                testLogger.atError().log("Received IOException trying to readAllBytes on '{}'", infile);
            }

            rval.add(apply(exampleInput, loglevel).toString());

            try {
                topNode = mapper.readTree(exampleInput);
                exampleSAV = generateRecordFromJson(topNode,
                                "com.jmcpdotcom.transform.UnwrapperTest.example", false);
                rval.add(apply(exampleSAV, loglevel).toString());
            } catch (JsonProcessingException jpe) {
                testLogger.error("Unable to readTree with file {}", exampleInput);
                assertThat(false);
            }

            if (prettyPrint) {
                String pretty = new StringBuilder()
                        .append(schemaPrettyPrint(exampleSAV.schema(), 1))
                        .toString();
                testLogger.atDebug().log("\n{}\n", pretty);
            }

        } catch (IOException ioe) {
            System.err.println("Unable to open " + infile);
        }
        return rval;
    }


    @Test
    public void withExample0() {
        final String examplefn = "/example0.json";
        final String testMetadata = "value=No Such Owner";

        /* Test strings without schema */
        final String testUUID = "data_UUID=b8562605-4f01-46de-a5c3-6d9e030c30b1";
        final String testMetaPos = "metadata_owner=Liger Software";
        final String testAddress = "data_Address=42 Wallaby Way Sydney NSW 2000";
        /* Test strings _with_ schema */
        final String testMetaPos_ws = "metadata_owner=Liger Software";
        final String testUUID_ws = "UUID=b8562605-4f01-46de-a5c3-6d9e030c30b1";
        final String testSingleLine_ws = "42 Wallaby Way Sydney NSW 2000";

        List<String> postProcData = infileToStrings(examplefn, "info", false);
        String first = postProcData.get(0);
        String second = postProcData.get(1);

        testLogger.atInfo().log("Flattened record is\n {}", first.replaceAll(",", "\n"));

        assertThat(first).contains(testUUID);
        assertThat(first).contains(testAddress);
        assertThat(first).doesNotContain(testMetadata);
        assertThat(first).contains(testMetaPos);

        assertThat(second).contains(testUUID_ws);
        assertThat(second).contains(testSingleLine_ws);
        assertThat(second).doesNotContain(testMetadata);
        assertThat(second).contains(testMetaPos_ws);
    }

    @Test
    public void withExample1() {
        final String examplefn = "/example1.json";
        final String testSource = "First Source Of Data";
        final String testMetadata = "value=No Such Owner";

        /* Test strings without schema */
        final String testUUID = "data_UUID=d857b0f4-33e7-4d22-b345-91c0062faee7";
        final String testMetaPos = "metadata_owner=Liger Software";
        /* Test strings _with_ schema */
        final String testMetaPos_ws = "owner=Liger Software";
        final String testUUID_ws = "data_UUID=d857b0f4-33e7-4d22-b345-91c0062faee7";

        List<String> postProcData = infileToStrings(examplefn, "info", false);
        String first = postProcData.get(0);
        String second = postProcData.get(1);

        assertThat(first).contains(testUUID);
        assertThat(first).contains(testSource);
        assertThat(first).doesNotContain(testMetadata);
        assertThat(first).contains(testMetaPos);

        assertThat(second).contains(testUUID_ws);
        assertThat(second).contains(testSource);
        assertThat(second).doesNotContain(testMetadata);
        assertThat(second).contains(testMetaPos_ws);
    }

    @Test
    public void withExample2() {
        final String examplefn = "/example2.json";
        final String testSource = "Geospatial provider 1";

        /* Test strings without schema */
        final String testDistanceMetres = "data_coastalDistanceMetres=478.39";
        final String testMetadata = "metadata_buildDate=2023-08-30";
        /* Test strings _with_ schema */
        final String testDistanceMetres_ws = "coastalDistanceMetres=478.39";
        final String testMetadata_ws = "metadata_owner=Liger Software";

        List<String> postProcData = infileToStrings(examplefn, "info", false);
        String first = postProcData.get(0);
        String second = postProcData.get(1);

        assertThat(first).contains(testDistanceMetres);
        assertThat(first).contains(testSource);
        assertThat(first).contains(testMetadata);

        assertThat(second).contains(testDistanceMetres_ws);
        assertThat(second).contains(testSource);
        assertThat(second).contains(testMetadata_ws);
    }

    @Test
    public void withExample3() {
        final String examplefn = "/example3.json";
        final String testMetadata = "metadata_owner=No Such Owner";
        final String testSource = "Another geospatial source";

        /* Test strings without schema */
        final String testCapacity = "data_capacityKV=132";
        final String testMetaPos = "metadata_owner=Liger Software";
        /* Test strings _with_ schema */
        final String testCapacity_ws = "capacityKV=132";
        final String testMetaPos_ws = "owner=Liger Software";

        List<String> postProcData = infileToStrings(examplefn, "info", false);
        String first = postProcData.get(0);
        String second = postProcData.get(1);

        assertThat(first).contains(testCapacity);
        assertThat(first).contains(testSource);
        assertThat(first).doesNotContain(testMetadata);
        assertThat(first).contains(testMetaPos);

        assertThat(second).contains(testCapacity_ws);
        assertThat(second).contains(testSource);
        assertThat(second).doesNotContain(testMetadata);
        assertThat(second).contains(testMetaPos_ws);
    }

    @Test
    public void withExample4() {
        final String examplefn = "/example4.json";
        final String testSource = "Xoning source of truth";
        final String testMetadata = "metadata_owner=No Such Owner";

        final String testUUID = "data_UUID=d857b0f4-33e7-4d22-b345-91c0062faee7";
        final String testMetaPos = "metadata_owner=Liger Software";

        List<String> postProcData = infileToStrings(examplefn, "info", false);
        String first = postProcData.get(0);
        String second = postProcData.get(1);

        assertThat(first).doesNotContain(testUUID);
        assertThat(first).contains(testSource);
        assertThat(first).doesNotContain(testMetadata);
        assertThat(first).contains(testMetaPos);

        assertThat(second).doesNotContain(testUUID);
        assertThat(second).contains(testSource);
        assertThat(second).doesNotContain(testMetadata);
        assertThat(second).contains(testMetaPos);
    }

    @Test
    public void withExample5() {
        final String examplefn = "/example5.json";

        /* tests without schema */
        final String testObjectPseudo0 = "anArray_0=1";
        final String testObjectPseudo1 = "anArray_2=3";
        final String testDistance = "data_distance=21.83";
        final String testArray = "data_randomEmbeddedObject_pseudoRandomObject_anArray_1=2";
        final String testMetadata0 = "metadata_source_1_version=2023.05";
        final String testMetadata1 = "metadata_source_0_version=Delivered after 2000";

        List<String> postProcData = infileToStrings(examplefn, "info", true);
        String first = postProcData.get(0);
        String second = postProcData.get(1);

        testLogger.atInfo().log("Flattened record is\n {}", first.replaceAll(",", "\n"));

        assertThat(first).contains(testObjectPseudo0);
        assertThat(first).contains(testObjectPseudo1);
        assertThat(first).contains(testDistance);
        assertThat(first).contains(testArray);

        assertThat(second).contains(testArray);
        assertThat(second).contains(testMetadata0);
        assertThat(second).contains(testMetadata1);
    }
}
