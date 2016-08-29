package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.JoinCondition;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkIEJoinOperator}.
 */
public class SparkIEJoinOperatorTest extends SparkOperatorTestBase {


    @Test
    public void testExecution() {
        Record r1 = new Record(100, 10);
        Record r2 = new Record(200, 20);
        Record r3 = new Record(300, 30);
        Record r11 = new Record(250, 5);
        // Prepare test data.
        RddChannel.Instance input0 = this.createRddChannelInstance(Arrays.asList(r1, r2, r3, r11));
        RddChannel.Instance input1 = this.createRddChannelInstance(Arrays.asList(r1, r2, r3));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Build the Cartesian operator.
        SparkIEJoinOperator<Integer, Integer> IEJoinOperator =
                new SparkIEJoinOperator<Integer, Integer>(
                        DataSetType.createDefaultUnchecked(Record.class),
                        DataSetType.createDefaultUnchecked(Record.class), 0, 0, JoinCondition.GreaterThan, 1, 1, JoinCondition.LessThan
                        /*new ProjectionDescriptor<Record, Integer>(
                                DataUnitType.createBasicUnchecked(Record.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<Record, Integer>(
                                DataUnitType.createBasicUnchecked(Record.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<Record, Integer>(
                                DataUnitType.createBasicUnchecked(Record.class),
                                DataUnitType.createBasic(Integer.class),
                                "field1"),
                        new ProjectionDescriptor<Record, Integer>(
                                DataUnitType.createBasicUnchecked(Record.class),
                                DataUnitType.createBasic(Integer.class),
                                "field1")*/
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input0, input1};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        IEJoinOperator.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        final List<Tuple2<Record, Record>> result = output.<Tuple2<Record, Record>>provideRdd().collect();
        Assert.assertEquals(2, result.size());
        //Assert.assertEquals(result.get(0), new Tuple2(1, "a"));

    }

}
