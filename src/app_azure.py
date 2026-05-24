from flask import Flask
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME")
    )

@app.route("/")
def index():
    try:
        conn = get_db_connection()
        df = pd.read_sql("SELECT * FROM patient_aggregated", conn)
        conn.close()

        if df.empty:
            return "<h1>No data available yet. Pipeline is warming up!</h1>"

        # Generate plot
        plt.figure(figsize=(10, 6))
        df.plot(x="Patient_ID", y="avg_heart_rate", kind="bar", color="skyblue")
        plt.title("Average Heart Rate per Patient")
        plt.ylabel("Heart Rate (bpm)")
        plt.tight_layout()

        # Save plot to memory
        img = io.BytesIO()
        plt.savefig(img, format="png")
        img.seek(0)
        plot_url = base64.b64encode(img.getvalue()).decode()
        plt.close()

        return f"<h1>Live Azure Patient Dashboard</h1><img src='data:image/png;base64,{plot_url}'/>"

    except Exception as e:
        return f"<h1>Database Connection Error:</h1><p>{str(e)}</p>"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
