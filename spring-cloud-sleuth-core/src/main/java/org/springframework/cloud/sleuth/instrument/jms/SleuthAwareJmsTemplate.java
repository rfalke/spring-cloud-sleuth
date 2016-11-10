package org.springframework.cloud.sleuth.instrument.jms;

import org.springframework.cloud.sleuth.Tracer;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

public class SleuthAwareJmsTemplate extends JmsTemplate {
    private final JmsSpanInjector spanInjector;
    private final Tracer tracer;

    public SleuthAwareJmsTemplate(ConnectionFactory connectionFactory, JmsSpanInjector spanInjector, Tracer tracer) {
        super(connectionFactory);
        this.spanInjector = spanInjector;
        this.tracer = tracer;
    }

    @Override
    protected void doSend(MessageProducer producer, Message message) throws JMSException {
        spanInjector.inject(tracer.getCurrentSpan(), message);
        super.doSend(producer, message);
    }
}
