package io.rheem.rheem.spark.mapping;

import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.spark.mapping.graph.PageRankMapping;

import java.util.Arrays;
import java.util.Collection;

/**
 * Register for {@link Mapping}s for this platform.
 */
public class Mappings {

    public static Collection<Mapping> BASIC_MAPPINGS = Arrays.asList(
            new TextFileSourceMapping(),
            new TextFileSinkMapping(),
            new MapMapping(),
            new MapPartitionsMapping(),
            new ReduceByMapping(),
            new CollectionSourceMapping(),
            new LocalCallbackSinkMapping(),
            new GlobalReduceMapping(),
            new MaterializedGroupByMapping(),
            new GlobalMaterializedGroupMapping(),
            new FlatMapMapping(),
            new CountMapping(),
            new DistinctMapping(),
            new SortMapping(),
            new FilterMapping(),
            new UnionAllMapping(),
            new IntersectMapping(),
            new CartesianMapping(),
            new JoinMapping(),
            new CoGroupMapping(),
            new LoopMapping(),
            new DoWhileMapping(),
            new RepeatMapping(),
            new SampleMapping(),
            new ZipWithIdMapping()
    );

    public static Collection<Mapping> GRAPH_MAPPINGS = Arrays.asList(
            new PageRankMapping()
    );

}
