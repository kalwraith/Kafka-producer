import sys
import time
import json
import logging
from confluent_kafka import Producer
from apis.seoul_data.realtime_bicycle import RealtimeBicycle
from datetime import datetime


BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'


class BicycleProducer():

    def __init__(self, topic):
        self.topic = topic
        self.conf = {'bootstrap.servers': BROKER_LST}
        self.producer = Producer(**self.conf)
        self._set_logger()

    def _set_logger(self):
        logging.basicConfig(
            format='%(asctime)s [%(levelname)s]:%(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.log = logging.getLogger(__name__)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(self, err, msg):
        if err:
            self.log.error('%% Message failed delivery: %s\n' % err)
        else:
            self.log.info('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def produce(self):
        rt_bycicle = RealtimeBicycle(dataset_nm='bikeList')
        while True:
            now_dt = datetime.now()
            items = rt_bycicle.call()
            for item in items:
                # 컬럼명 변경
                item['STT_ID'] = item.pop('stationId')
                item['STT_NM'] = item.pop('stationName')
                item['TOT_RACK_CNT'] = item.pop('rackToCnt')
                item['TOT_PRK_CNT'] = item.pop('parkingBikeTotCnt')
                item['RT_PRK_RACK'] = item.pop('shared')
                item['STT_LTTD'] = item.pop('stationLatitude')
                item['STT_LGTD'] = item.pop('stationLongitude')


                # 컬럼 추가
                item['CRT_DTTM'] = now_dt

                # produce
                try:
                    self.producer.produce(
                        topic=self.topic,
                        key=json.dumps({'STT_NM',item['STT_NM']}),
                        value=json.dumps(item),
                        on_delivery=self.delivery_callback
                    )

                except BufferError:
                    self.log.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                     len(self.producer))

                # Serve delivery callback queue.
                # NOTE: Since produce() is an asynchronous API this poll() call
                #       will most likely not serve the delivery callback for the
                #       last produce()d message.
                self.producer.poll(0)

            # Wait until all messages have been delivered
            self.log.info('%% Waiting for %d deliveries\n' % len(self.producer))
            self.producer.flush()

            # 15분 대기
            time.sleep(60*15)


if __name__ == '__main__':
    producer = Producer(topic='apis.seouldata.rt-bicycle')
    producer.produce()