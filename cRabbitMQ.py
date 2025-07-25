import pika

class classRabbitMQ:
    _instance = None

    def __init__(self):
        rmq_host = 'amqp://jaragua-01.lmq.cloudamqp.com'
        rmq_port = 5672
        rmq_user = 'bjnuffmq'
        rmq_pass = 'gj-YQIiEXyfxQxjsZtiYDKeXIT8ppUq7'
        self.channel = None
        credentials = pika.PlainCredentials(rmq_user, rmq_pass)
        rmq_param = pika.ConnectionParameters(
            host=rmq_host,
            port=rmq_port,
            credentials=credentials
        )

        connection = pika.BlockingConnection(rmq_param)
        channel = connection.channel()

    @classmethod
    def get_instance(cls):
        if cls._instance is None or cls._instance.connection.is_closed:
            cls._instance = classRabbitMQ()
        return cls._instance

    def get_channel(self):
        return self.channel