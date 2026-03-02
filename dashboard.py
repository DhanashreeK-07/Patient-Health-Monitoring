from flask import Flask, render_template, jsonify
import pandas as pd
import os

app = Flask(__name__)

def load_data():
    folder = "spark_output"
    files = [f for f in os.listdir(folder) if f.endswith(".csv")]

    all_data = []
    for file in files:
        df = pd.read_csv(os.path.join(folder, file), header=None)
        all_data.extend(df[0].tolist())

    return all_data[-20:]  # last 20 values

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/data")
def data():
    values = load_data()
    labels = list(range(len(values)))
    return jsonify({"labels": labels, "values": values})

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=5000,debug=True)
