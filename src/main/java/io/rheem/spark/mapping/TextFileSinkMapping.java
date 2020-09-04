package io.rheem.spark.mapping;

import io.rheem.basic.operators.CollectionSource;
import io.rheem.basic.operators.TextFileSink;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.spark.operators.SparkCollectionSource;
import io.rheem.spark.operators.SparkTextFileSink;
import io.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link CollectionSource} to {@link SparkCollectionSource}.
 */
public class TextFileSinkMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                SparkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "sink", new TextFileSink<>("", DataSetType.none().getDataUnitType().getTypeClass()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<TextFileSink<?>>(
                (matchedOperator, epoch) -> new SparkTextFileSink<>(matchedOperator).at(epoch)
        );
    }
}
