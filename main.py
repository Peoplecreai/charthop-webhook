from flask import Flask, request

app = Flask(__name__)

@app.route("/", methods=["POST", "GET"])
def webhook():
    if request.method == "POST":
        # Aquí llegará el JSON de ChartHop
        print(request.json)
        return "", 200
    return "ChartHop webhook up", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
