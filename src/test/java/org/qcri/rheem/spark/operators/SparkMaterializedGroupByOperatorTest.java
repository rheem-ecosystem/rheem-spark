package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link SparkMaterializedGroupBy}.
 */
public class SparkMaterializedGroupByOperatorTest {

    @Test
    public void testExecution() {
//        // Prepare test data.
//        AtomicInteger counter = new AtomicInteger(0);
//        Stream<Tuple2<String, Integer>> inputStream = Arrays.stream("abcaba".split(""))
//                .map(string -> new Tuple2<>(string, counter.getAndIncrement()));
//
//        // Build the reduce operator.
//        JavaMaterializedGroupByOperator<Tuple2<String, Integer>, String> collocateByOperator =
//                new JavaMaterializedGroupByOperator<>(
//                        DataSetType.createDefaultUnchecked(Tuple2.class),
//                        new ProjectionDescriptor<>(
//                                DataUnitType.createBasicUnchecked(Tuple2.class),
//                                DataUnitType.createBasicUnchecked(Tuple2.class),
//                                "field0")
//                );
//
//        // Execute the reduce operator.
//        final Stream[] outputStreams = collocateByOperator.evaluate(new Stream[]{inputStream}, new FunctionCompiler());
//
//        // Verify the outcome.
//        Assert.assertEquals(1, outputStreams.length);
//        final Set<List<Tuple2<String, Integer>>> result =
//                ((Stream<List<Tuple2<String, Integer>>>) outputStreams[0]).collect(Collectors.toSet());
//        final List[] expectedResults = {
//                Arrays.asList(new Tuple2<>("a", 0), new Tuple2<>("a", 3), new Tuple2<>("a", 5)),
//                Arrays.asList(new Tuple2<>("b", 1), new Tuple2<>("b", 4)),
//                Arrays.asList(new Tuple2<>("c", 2))
//        };
//        Arrays.stream(expectedResults)
//                .forEach(expected -> Assert.assertTrue("Not contained: " + expected, result.contains(expected)));
//        Assert.assertEquals(expectedResults.length, result.size());

    }
}
