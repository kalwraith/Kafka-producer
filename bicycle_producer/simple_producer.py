from confluent_kafka import Producer
from datetime import datetime
import sys
import time


class SimpleProducer:
    def __init__(self, broker_lst, topic):
        self.broker_lst = broker_lst
        self.topic = topic
        self.conf = {'bootstrap.servers': broker_lst}

        self.producer = Producer(**self.conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def produce(self):
        cnt = 0
        while cnt < 60:
            msg = f'{datetime.now()}, {cnt}'
            try:
                # Produce line (without newline)
                self.producer.produce(
                    topic=self.topic,
                    key=str(cnt),
                    value=f'hello world: {cnt}',
                    on_delivery=self.delivery_callback)

            except BufferError:
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                 len(self.producer))

            # Serve delivery callback queue.
            # NOTE: Since produce() is an asynchronous API this poll() call
            #       will most likely not serve the delivery callback for the
            #       last produce()d message.
            self.producer.poll(0)
            cnt += 1
            time.sleep(1)  # 1초 대기

        # Wait until all messages have been delivered
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer))
        self.producer.flush()
