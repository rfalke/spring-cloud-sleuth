package org.springframework.cloud.sleuth.instrument.jms;

import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanExtractor;

import javax.jms.JMSException;
import javax.jms.Message;
import java.util.Random;

public class JmsSpanExtractor implements SpanExtractor<Message> {
    private final Random random;

    public JmsSpanExtractor(Random random) {
        this.random = random;
    }

    @Override
    public Span joinTrace(Message carrier) {
        if (hasHeader(carrier, Span.TRACE_ID_NAME) && hasHeader(carrier, Span.SPAN_ID_NAME)) {
            return extractSpanFromHeaders(carrier, Span.builder());
        }
        return null;
    }

    private Span extractSpanFromHeaders(Message carrier, Span.SpanBuilder spanBuilder) {
        long traceId = getIdHeader(carrier, Span.TRACE_ID_NAME);
        long spanId = hasHeader(carrier, Span.SPAN_ID_NAME)
                ? getIdHeader(carrier, Span.SPAN_ID_NAME)
                : this.random.nextLong();
        spanBuilder = spanBuilder.traceId(traceId).spanId(spanId);
        spanBuilder.exportable(
                Span.SPAN_SAMPLED.equals(getHeader(carrier, Span.SAMPLED_NAME)));
        String processId = getHeader(carrier, Span.PROCESS_ID_NAME);
        String spanName = getHeader(carrier, Span.SPAN_NAME_NAME);
        if (spanName != null) {
            spanBuilder.name(spanName);
        }
        if (processId != null) {
            spanBuilder.processId(processId);
        }
        if (hasHeader(carrier, Span.PARENT_ID_NAME)) {
            spanBuilder.parent(getIdHeader(carrier, Span.PARENT_ID_NAME));
        }
        spanBuilder.remote(true);
        return spanBuilder.build();
    }

    private String getHeader(Message message, String name) {
        try {
            return message.getStringProperty(name);
        } catch (JMSException e) {
            throw new RuntimeException("Failed to get the string property " + name);
        }
    }

    private long getIdHeader(Message message, String name) {
        try {
            return message.getLongProperty(name);
        } catch (JMSException e) {
            throw new RuntimeException("Failed to get the string property " + name);
        }
    }

    private boolean hasHeader(Message message, String name) {
        try {
            return message.propertyExists(name);
        } catch (JMSException e) {
            throw new RuntimeException("Failed to test for property " + name);
        }
    }

}
