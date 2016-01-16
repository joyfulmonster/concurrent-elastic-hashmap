package org.joyfulmonster.util.concurrent.internal;

/**
 * An runtime error indicating a Bucket is overflowed.
 *
 * Created by wbao on 1/11/2016.
 */
public class BucketOverflowError extends IllegalStateException {
    public BucketOverflowError() {
        super("The bucket does not have enough capacity");
    }
}

