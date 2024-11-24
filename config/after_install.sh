source /src/kafka_venv/bin/activate
pip3 install -r /src/kafka-producer/requirements.txt > /src/kafka-producer/codedeploy.log
python3 /src/kafka-producer/config/deploy.py > /src/kafka-producer/codedeploy.log