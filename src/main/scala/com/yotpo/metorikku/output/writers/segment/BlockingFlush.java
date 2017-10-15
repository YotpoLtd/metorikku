package com.yotpo.metorikku.output.writers.segment;

import com.segment.analytics.Analytics;
import com.segment.analytics.Callback;
import com.segment.analytics.MessageTransformer;
import com.segment.analytics.Plugin;
import com.segment.analytics.messages.Message;
import com.segment.analytics.messages.MessageBuilder;
import java.util.concurrent.Phaser;

/**
 * The {@link Analytics} class doesn't come with a blocking {@link Analytics#flush()} implementation
 * out of the box. It's trivial to build one using a {@link Phaser} that monitors requests and is
 * able to block until they're uploaded.
 *
 * <pre><code>
 * BlockingFlush blockingFlush = BlockingFlush.create();
 * Analytics analytics = Analytics.builder(writeKey)
 *      .plugin(blockingFlush)
 *      .build();
 *
 * // Do some work.
 *
 * analytics.flush(); // Trigger a flush.
 * blockingFlush.block(); // Block until the flush completes.
 * analytics.shutdown(); // Shut down after the flush is complete.
 * </code></pre>
 */
public class BlockingFlush {

    public static BlockingFlush create() {
        return new BlockingFlush();
    }

    BlockingFlush() {
        this.phaser = new Phaser(1);
    }

    final Phaser phaser;

    public Plugin plugin() {
        return new Plugin() {
            @Override public void configure(Analytics.Builder builder) {
                builder.messageTransformer(new MessageTransformer() {
                    @Override public boolean transform(MessageBuilder builder) {
                        phaser.register();
                        return true;
                    }
                });

                builder.callback(new Callback() {
                    @Override public void success(Message message) {
                        phaser.arrive();
                    }

                    @Override public void failure(Message message, Throwable throwable) {
                        phaser.arrive();
                    }
                });
            }
        };
    }

    public void block() {
        phaser.arriveAndAwaitAdvance();
    }
}