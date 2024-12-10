if [ "$HOSTNAME" == "kafka01" ]; then
  systemctl stop bicycle-producer.service
  systemctl start bicycle-producer.service
fi