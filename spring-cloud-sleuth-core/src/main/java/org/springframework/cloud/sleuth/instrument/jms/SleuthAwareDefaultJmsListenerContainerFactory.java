package org.springframework.cloud.sleuth.instrument.jms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.lang.invoke.MethodHandles;

public class SleuthAwareDefaultJmsListenerContainerFactory extends DefaultJmsListenerContainerFactory {
    private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

    private final JmsSpanExtractor spanExtractor;
    private final Tracer tracer;

    public SleuthAwareDefaultJmsListenerContainerFactory(JmsSpanExtractor spanExtractor, Tracer tracer) {
        this.spanExtractor = spanExtractor;
        this.tracer = tracer;
    }

    @Override
    protected DefaultMessageListenerContainer createContainerInstance() {
        return new DefaultMessageListenerContainer() {
            @Override
            protected void invokeListener(Session session, Message message) throws JMSException {
                String name = "jms:" + message.getJMSDestination();
                Span spanFromRequest = createSpan(message, name);
                try {
                    super.invokeListener(session, message);
                } finally {
                    closeSpans(spanFromRequest);
                }
            }

            private Span createSpan(Message message, String name) {
                Span parent = spanExtractor.joinTrace(message);
                Span result;
                if (parent != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Found a parent span " + parent + " in the request");
                    }
                    result = tracer.createSpan(name, parent);
                    if (parent.isRemote()) {
                        result.logEvent(Span.SERVER_RECV);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Parent span is " + parent + "");
                    }
                } else {
                    result = tracer.createSpan(name);
                    result.logEvent(Span.SERVER_RECV);
                    if (log.isDebugEnabled()) {
                        log.debug("No parent span present - creating a new span");
                    }
                }
                return result;
            }

            private void closeSpans(Span span) {
                if (span != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Closing the span " + span + " since the response was successful");
                    }
                    tracer.close(span);
                }
            }
        };
    }
}
