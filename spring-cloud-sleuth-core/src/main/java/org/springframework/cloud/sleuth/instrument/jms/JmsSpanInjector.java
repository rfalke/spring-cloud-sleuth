package org.springframework.cloud.sleuth.instrument.jms;

import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanInjector;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.jms.JMSException;
import javax.jms.Message;

@Component
public class JmsSpanInjector implements SpanInjector<Message> {
    @Override
    public void inject(Span span, Message carrier) {
        setIdHeader(carrier, Span.TRACE_ID_NAME, span.getTraceId());
        setIdHeader(carrier, Span.SPAN_ID_NAME, span.getSpanId());
        setHeader(carrier, Span.SAMPLED_NAME, span.isExportable() ? Span.SPAN_SAMPLED : Span.SPAN_NOT_SAMPLED);
        setHeader(carrier, Span.SPAN_NAME_NAME, span.getName());
        setIdHeader(carrier, Span.PARENT_ID_NAME, getParentId(span));
        setHeader(carrier, Span.PROCESS_ID_NAME, span.getProcessId());
    }

    private void setHeader(Message message, String name, String value) {
        try {
            if (StringUtils.hasText(value) && !message.propertyExists(name)) {
                message.setStringProperty(name, value);
            }
        } catch (JMSException e) {
            throw new RuntimeException("Failed to set property " + name + " to " + value + " for " + message);
        }
    }

    private void setIdHeader(Message message, String name, Long value) {
        try {
            if (value != null && !message.propertyExists(name)) {
                message.setLongProperty(name, value);
            }
        } catch (JMSException e) {
            throw new RuntimeException("Failed to set property " + name + " to " + value + " for " + message);
        }
    }

    private Long getParentId(Span span) {
        return !span.getParents().isEmpty() ? span.getParents().get(0) : null;
    }

}
