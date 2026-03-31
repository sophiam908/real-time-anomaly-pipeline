FROM python:3.10-slim

WORKDIR /app

COPY . /app/

RUN pip install --no-cache-dir boto3 pandas scikit-learn pyarrow

CMD ["python", "train_model.py"]
