import requests
import json
from json.decoder import JSONDecodeError
import logging
from logging.handlers import TimedRotatingFileHandler
import time
import os
import traceback


class RealtimeBicycle:

    def __init__(self, dataset_nm):
        self.auth_key = '##auth_key_seoul_data##'
        self.api_url = 'http://openapi.seoul.go.kr:8088'
        self.log_dir = '/log/seoul_api'
        self.dataset_nm = dataset_nm
        self.chk_dir()
        self.log = self._get_logger()

    def call(self):
        # url 형태: http://openapi.seoul.go.kr:8088/(인증키)/json/bikeList/1/5/
        base_url = f'{self.api_url}/{self.auth_key}/json/{self.dataset_nm}'
        start = 1
        end = 1000
        total_rows = []
        while True:
            try:
                rslt = self._call_api(base_url, start, end)
                contents = json.loads(rslt.text)
            except JSONDecodeError:
                # url 요청 실패시 XML 형태로 에러 내용 리턴됨
                self.log.error(f'요청 실패, {traceback.format_exc()}')
                time.sleep(30)  # 30초 대기 후 재시도
                continue


            # 정상이 아닌 경우 처리
            rslt_code = contents.get('CODE')
            if rslt_code:
                # INFO-200: 해당하는 데이터 없음. total_rows 리스트에 값이 존재할 경우
                # 조회 범위 초과로 에러 발생한 것이며 결과 리턴하고 종료
                if rslt_code == 'INFO-200' and total_rows:
                    return total_rows
                else:
                    rslt_msg = contents.get('MESSAGE')
                    self.log.error(f'요청 실패, 에러코드: {rslt_code}, 메시지:{rslt_msg}')
                    time.sleep(30)      # 30초 대기

            key_nm = list(contents.keys())[0]
            items = contents.get(key_nm)
            item_cnt = items.get('list_total_count')
            item_row = items.get('row')
            self.log.info(f'{base_url}/{start}/{end} 조회 성공, 건수: {len(item_row)}')
            if item_row:
                total_rows += item_row
            if item_cnt < 1000:
                break
            else:
                start = end + 1
                end += 1000
        return total_rows

    def _call_api(self, base_url, start, end, base_dt=''):
        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }
        if len(base_dt) > 0:
            url = f'{base_url}/{start}/{end}/{base_dt}'
        else:
            url = f'{base_url}/{start}/{end}'
        rslt = requests.get(url, headers)
        return rslt

    def chk_dir(self):
        os.makedirs(self.log_dir, exist_ok=True)

    def _get_logger(self):
        logging.basicConfig(
            format='%(asctime)s [%(levelname)s]:%(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler = TimedRotatingFileHandler(os.path.join(self.log_dir, 'call_bicycle_api.log'), when="midnight", backupCount=7)
        handler.suffix = "%Y-%m-%d"
        logger = logging.getLogger(__name__)
        logger.addHandler(handler)

        return logger


real_bicycle = RealtimeBicycle(dataset_nm='bikeList')
items = real_bicycle.call()