from flask import Flask, render_template
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64

app = Flask(__name__)

@app.route("/")
def index():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="health_db"
    )

    df = pd.read_sql("SELECT * FROM patient_aggregated", conn)

    plt.figure()
    df.plot(x="Patient_ID", y="avg_heart_rate", kind="bar")

    img = io.BytesIO()
    plt.savefig(img, format="png")
    img.seek(0)

    plot_url = base64.b64encode(img.getvalue()).decode()

    return f"<img src='data:image/png;base64,{plot_url}'/>"

if __name__ == "__main__":
    app.run(debug=True)