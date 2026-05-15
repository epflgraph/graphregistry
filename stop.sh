echo "Stopping all services..."
systemctl --user stop promtail.service
systemctl --user stop uvicorn-api-graphregistry.service
echo "All services stopped."