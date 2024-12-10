if [ "$HOSTNAME" == "kafka01" ]; then
  systemctl restart bicycle-producer.service
fi