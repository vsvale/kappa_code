# import libraries

# get env
load_dotenv()

# load variables
kafka_client_id_json = os.getenv("KAFKA_CLIENT_TO_JSON")
kafka_bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")

# flask app
app = FLASK(__none__)

# post into api
@app.route('/json', methods=['POST'])
def json():
    events = request.json
    topic = "src-app-skin-user-json"
    Kafka().json_producer(broker=kafka_bootstrap_server,events=events, kafka_topic=topic)
    return jsonify(events)

# main
if __name__ == "main":
    app.run(debug=True, host=config.host, port=config.port)