package org.qcri.rheem.spark.mapping;

import org.qcri.rheem.basic.operators.MapPartitionsOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.operators.SparkMapPartitionsOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

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
