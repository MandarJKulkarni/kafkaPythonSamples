import random
from flask import Flask

app = Flask(__name__)


@app.route("/")
def hello():
    html = "<h3>Hello, World!</h3>"
    return html


class CustomPartitioner:
    @classmethod
    def __call__(cls, key, all_partitions, available):
        if key == b'tenant0':
            return all_partitions[0]
        elif key == b'tenant1':
            return all_partitions[1]
        elif key == b'tenant2':
            return all_partitions[2]
        return random.choice(all_partitions)


@app.route("/publishToTopic/<topic>/<message>/<key>")
def publish(topic, message, key):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers='localhost:9092', partitioner=CustomPartitioner())
    producer.send(topic=topic, value=bytes(message, 'utf-8'), key=bytes(key, 'utf-8'), headers=[('header1', b'value1')])
    info_message = 'published '+ message + ' to '+ topic + ' with key ' + key
    print(info_message)
    return message


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
