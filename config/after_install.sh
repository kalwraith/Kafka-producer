source /src/kafka_venv/bin/activate
python3 /src/kafka-producer/config/deploy.py
pip3 install -r /src/kafka-producer/requirements.txt

# kafka01 서버 bicycle-producer restart
if [ "$HOSTNAME" == "kafka01" ]; then
  systemctl stop bicycle-producer.service
  systemctl start bicycle-producer.service
fi