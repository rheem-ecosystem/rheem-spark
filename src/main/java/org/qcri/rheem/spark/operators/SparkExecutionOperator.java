package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Collection;

/**
 * Execution operator for the {@link SparkPlatform}.
 */
public interface SparkExecutionOperator extends ExecutionOperator {

    @Override
    default SparkPlatform getPlatform() {
        return SparkPlatform.getInstance();
    }

    /**
     * Evaluates this operator. Takes a set of {@link ChannelInstance}s according to the operator inputs and manipulates
     * a set of {@link ChannelInstance}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     * <p>In addition, this method should give feedback of what this instance was doing by wiring the
     * {@link org.qcri.rheem.core.platform.LazyChannelLineage} of input and ouput {@link ChannelInstance}s and
     * providing a {@link Collection} of executed {@link OptimizationContext.OperatorContext}s.</p>
     *
     * @param inputs          {@link ChannelInstance}s that satisfy the inputs of this operator
     * @param outputs         {@link ChannelInstance}s that accept the outputs of this operator
     * @param sparkExecutor   {@link SparkExecutor} that executes this instance
     * @param operatorContext optimization information for this instance
     * @return a {@link Collection} of what has been executed
     */
    Collection<OptimizationContext.OperatorContext> evaluate(ChannelInstance[] inputs,
                                                             ChannelInstance[] outputs,
                                                             SparkExecutor sparkExecutor,
                                                             OptimizationContext.OperatorContext operatorContext);

    /**
     * Utility method to name an RDD according to this instance's name.
     *
     * @param rdd that should be renamed
     * @see #getName()
     */
    default void name(JavaRDD<?> rdd) {
        if (this.getName() != null) {
            rdd.setName(this.getName());
        } else {
            rdd.setName(this.toString());
        }
    }

    /**
     * Utility method to name an RDD according to this instance's name.
     *
     * @param rdd that should be renamed
     * @see #getName()
     */
    default void name(JavaPairRDD<?, ?> rdd) {
        if (this.getName() != null) {
            rdd.setName(this.getName());
        } else {
            rdd.setName(this.toString());
        }
    }

    /**
     * Utility method to forward a {@link RddChannel.Instance} to another.
     *
     * @param input  that should be forwarded
     * @param output to that should be forwarded
     */
    static void forward(ChannelInstance input, ChannelInstance output, SparkExecutor sparkExecutor) {
        final RddChannel.Instance rddInput = (RddChannel.Instance) input;
        final RddChannel.Instance rddOutput = (RddChannel.Instance) output;

        // Do the forward.
        assert rddInput.getChannel().getDescriptor() != RddChannel.CACHED_DESCRIPTOR ||
                rddOutput.getChannel().getDescriptor() == RddChannel.CACHED_DESCRIPTOR;
        rddOutput.accept(rddInput.provideRdd(), sparkExecutor);

        // Manipulate the lineage.
        output.getLazyChannelLineage().copyRootFrom(input.getLazyChannelLineage());
    }

}
