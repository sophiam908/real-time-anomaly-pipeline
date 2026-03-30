from fastapi import FastAPI
import json

app = FastAPI()

@app.get("/")
def home():
    return {"message": "API is running"}

@app.get("/data")
def get_data():
    try:
        with open("data/data_store.json", "r") as f:
            data = json.load(f)
            return data[-10:]  # 🔥 only last 10
    except:
        return []
    

@app.get("/latest")
def latest():
    try:
        with open("data/data_store.json", "r") as f:
            data = json.load(f)
            return data[-1] if data else {}
    except:
        return {}