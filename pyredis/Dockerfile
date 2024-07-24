FROM python:3.11-slim
RUN apt-get update && apt-get update
WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY k8srdis.py . 
EXPOSE 8005
CMD ["python3","k8srdis.py"]