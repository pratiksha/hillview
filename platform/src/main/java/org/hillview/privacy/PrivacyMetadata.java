package org.hillview.privacy;

import org.hillview.dataset.api.IJson;

public class PrivacyMetadata implements IJson {
    /**
     * Total privacy budget allotted to this column.
     */
    public double epsilon;

    /**
     * Data type, for deserializing metadata for different types.
     */
    public enum Type {
        Numeric,
        String
    }

    public PrivacyMetadata(double epsilon) {
        this.epsilon = epsilon;
    }
}
