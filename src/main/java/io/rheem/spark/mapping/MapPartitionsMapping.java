package io.rheem.spark.mapping;

import io.rheem.basic.operators.MapPartitionsOperator;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.spark.operators.SparkMapPartitionsOperator;
import io.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MapPartitionsOperator} to {@link SparkMapPartitionsOperator}.
 */
@SuppressWarnings("unchecked")
public class MapPartitionsMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singletonList(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        SparkPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "mapPartitions", new MapPartitionsOperator<>(null, DataSetType.none(), DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapPartitionsOperator>(
                (matchedOperator, epoch) -> new SparkMapPartitionsOperator<>(matchedOperator).at(epoch)
        );
    }

}
