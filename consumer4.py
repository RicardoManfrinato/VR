from app import dictMsg, rmq_dead, AddToDic
from cRabbitMQ import classRabbitMQ
import json


def callback(ch, method, properties, body):
    mensagem = json.loads(body)

    traceId = mensagem["traceId"]
    mensagemId = mensagem["mensagemId"]
    conteudoMensagem = mensagem["conteudoMensagem"]
    tipoNotificacao = mensagem["tipoNotificacao"]
    status = 'FALHA_ENVIO_FINAL'
    AddToDic(traceId, mensagemId, conteudoMensagem, tipoNotificacao, status)


def main():
    client = classRabbitMQ.get_instance()
    channel = client.get_channel()

    # Declaração da fila
    channel.queue_declare(queue=rmq_dead, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=rmq_dead, on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    main()
