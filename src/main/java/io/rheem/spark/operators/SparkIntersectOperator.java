package io.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import io.rheem.basic.operators.IntersectOperator;
import io.rheem.basic.operators.JoinOperator;
import io.rheem.core.optimizer.OptimizationContext;
import io.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.core.platform.ChannelDescriptor;
import io.rheem.core.platform.ChannelInstance;
import io.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.core.types.DataSetType;
import io.rheem.core.util.Tuple;
import io.rheem.spark.channels.RddChannel;
import io.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Spark implementation of the {@link JoinOperator}.
 */
public class SparkIntersectOperator<Type> extends IntersectOperator<Type> implements SparkExecutionOperator {

    public SparkIntersectOperator(DataSetType<Type> dataSetType) {
        super(dataSetType);
    }

    public SparkIntersectOperator(Class<Type> typeClass) {
        super(typeClass);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkIntersectOperator(IntersectOperator<Type> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input0 = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance input1 = (RddChannel.Instance) inputs[1];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<Object> outputRdd = input0.provideRdd().intersection(input1.provideRdd());
        this.name(outputRdd);
        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkIntersectOperator<>(this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.intersect.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
