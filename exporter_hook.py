from flask import Flask
import cloudwatch_metrics

app = Flask(__name__)

@app.route("/metrics", methods=['GET'])
def get_metrics():
   data = cloudwatch_metrics.rds_metric()
   print(data)
   return data


if __name__ == '__main__':
   app.run(host='localhost', port='9106')