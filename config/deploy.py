import yaml
import os

root_dir = '/src/kafka-producer'
with open(f'{root_dir}/config/application.yml') as config:
    all_conf_dict = yaml.load(config, Loader=yaml.FullLoader)

for (root, dirs, files) in os.walk(root_dir):
    # root는 문자열로, dirs와 files는 리스트로 리턴됨
    if root.find('/.git') > 0 or root.find('/config/') > 0 or root.find('/.github/') > 0:
        # 치환 불필요 디렉토리
        continue

    # files 가 1개 이상 있을 경우 실행
    if len(files) > 0:
        for file in files:
            # 치환 대상은 .py 파일만 대상으로 함
            if file.endswith('.py'):
                with open(f'{root}/{file}', 'r', encoding='utf-8') as file_read:
                    py_file_all = ''.join(file_read.readlines())
                    with open(f'{root}/{file}', 'w', encoding='utf-8') as file_write:

                        # application.yml 파일의 모든 키, 값 쌍을 읽어 replace
                        for conf_item_dict in list(all_conf_dict.values()):
                            for k, v in conf_item_dict.items():
                                py_file_all = py_file_all.replace(f'##{k}##',v)
                            file_write.write(py_file_all)



