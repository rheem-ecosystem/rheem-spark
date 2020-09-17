package io.rheem.spark.compiler;

import io.rheem.core.function.FunctionDescriptor;
import org.apache.spark.api.java.function.Function;
import io.rheem.core.function.ExecutionContext;
import io.rheem.spark.execution.SparkExecutionContext;

/**
 * Implements a {@link Function} that calls {@link io.rheem.core.function.ExtendedFunction#open(ExecutionContext)}
 * of its implementation before delegating the very first {@link Function#call(Object)}.
 */
public class ExtendedPredicateAdapater<Type> implements Function<Type, Boolean> {

    private final FunctionDescriptor.ExtendedSerializablePredicate<Type> impl;

    private final SparkExecutionContext executionContext;

    private boolean isFirstRun = true;

    public ExtendedPredicateAdapater(FunctionDescriptor.ExtendedSerializablePredicate<Type> extendedFunction,
                                     SparkExecutionContext sparkExecutionContext) {
        this.impl = extendedFunction;
        this.executionContext = sparkExecutionContext;
    }

    @Override
    public Boolean call(Type dataQuantume) throws Exception {
        if (this.isFirstRun) {
            this.impl.open(this.executionContext);
        }

        return this.impl.test(dataQuantume);
    }

}
