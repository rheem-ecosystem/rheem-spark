package io.rheem.spark.operators;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import io.rheem.core.api.Configuration;
import io.rheem.core.api.Job;
import io.rheem.core.optimizer.DefaultOptimizationContext;
import io.rheem.core.optimizer.OptimizationContext;
import io.rheem.core.plan.rheemplan.Operator;
import io.rheem.core.platform.ChannelInstance;
import io.rheem.core.platform.CrossPlatformExecutor;
import io.rheem.core.profiling.FullInstrumentationStrategy;
import io.rheem.java.channels.CollectionChannel;
import io.rheem.spark.channels.RddChannel;
import io.rheem.spark.execution.SparkExecutor;
import io.rheem.spark.platform.SparkPlatform;
import io.rheem.spark.test.ChannelFactory;

import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test base for {@link SparkExecutionOperator} tests.
 */
public class SparkOperatorTestBase {

    protected Configuration configuration;

    protected SparkExecutor sparkExecutor;

    @Before
    public void setUp() {
        this.configuration = new Configuration();
        this.sparkExecutor = (SparkExecutor) SparkPlatform.getInstance().getExecutorFactory().create(this.mockJob());
    }

    Job mockJob() {
        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(this.configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new FullInstrumentationStrategy()));
        return job;
    }

    protected OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = new DefaultOptimizationContext(mockJob());
        return optimizationContext.addOneTimeOperator(operator);
    }

    protected void evaluate(SparkExecutionOperator operator,
                            ChannelInstance[] inputs,
                            ChannelInstance[] outputs) {
        operator.evaluate(inputs, outputs, this.sparkExecutor, this.createOperatorContext(operator));
    }

    RddChannel.Instance createRddChannelInstance() {
        return ChannelFactory.createRddChannelInstance(this.configuration);
    }

    RddChannel.Instance createRddChannelInstance(Collection<?> collection) {
        return ChannelFactory.createRddChannelInstance(collection, this.sparkExecutor, this.configuration);
    }

    protected CollectionChannel.Instance createCollectionChannelInstance() {
        return ChannelFactory.createCollectionChannelInstance(this.configuration);
    }

    protected CollectionChannel.Instance createCollectionChannelInstance(Collection<?> collection) {
        return ChannelFactory.createCollectionChannelInstance(collection, this.configuration);
    }

    public JavaSparkContext getSC() {
        return this.sparkExecutor.sc;
    }

}
