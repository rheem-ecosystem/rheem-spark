package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.SortOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.spark.operators.SparkSortOperator;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SortOperator} to {@link SparkSortOperator}.
 */
public class SortToSparkSortMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(createSubplanPattern(), new ReplacementFactory()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "sort", new SortOperator<>(null), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final SortOperator<?> originalOperator = (SortOperator<?>) subplanMatch.getMatch("sort").getOperator();
            return new SparkSortOperator<>(originalOperator.getInputType()).at(epoch);
        }
    }
}
