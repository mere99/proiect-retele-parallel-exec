FROM python:3.12-alpine

WORKDIR /app

COPY protocol.py cluster_state.py executor.py node.py ./
COPY tasks/ ./tasks/

ENV PYTHONUNBUFFERED=1

CMD ["python", "node.py"]
