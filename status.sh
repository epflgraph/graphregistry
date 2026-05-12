echo "Service status:"
systemctl --user --no-pager status promtail.service
systemctl --user --no-pager status uvicorn-api-graphregistry.service
