package io.rheem.spark.plugin;

import io.rheem.core.api.Configuration;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.core.platform.Platform;
import io.rheem.core.plugin.Plugin;
import io.rheem.java.platform.JavaPlatform;
import io.rheem.spark.channels.ChannelConversions;
import io.rheem.spark.platform.SparkPlatform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} enables to create {@link org.apache.spark.rdd.RDD}s.
 */
public class SparkConversionPlugin implements Plugin {

    @Override
    public Collection<Mapping> getMappings() {
        return Collections.emptyList();
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return ChannelConversions.ALL;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(SparkPlatform.getInstance(), JavaPlatform.getInstance());
    }

    @Override
    public void setProperties(Configuration configuration) {
        // Nothing to do, because we already configured the properties in #configureDefaults(...).
    }

}
