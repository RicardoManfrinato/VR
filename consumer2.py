from app import dictMsg, rmq_retry, rmq_validacao, rmq_dead, AddToDic
from flask import jsonify
from cRabbitMQ import classRabbitMQ
import pika
import json
import time
import random


def enviar_mensagem(message, fila):
    client = classRabbitMQ.get_instance()
    channel = client.get_channel()

    # Declaração da fila
    channel.queue_declare(queue=fila, durable=True)

    # Envia a mensagem
    channel.basic_publish(
        exchange='',
        routing_key=fila,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
        )
    )


def callback(ch, method, properties, body):
    # Simulando o delay
    time.sleep(3)

    mensagem = json.loads(body)

    chance_falha = random.randint(1, 100)
    traceId = mensagem["traceId"]
    mensagemId = mensagem["mensagemId"]
    conteudoMensagem = mensagem["conteudoMensagem"]
    tipoNotificacao = mensagem["tipoNotificacao"]
    status =  mensagem["status"]

    if chance_falha == 20:
        #Falha simulada
        status = 'FALHA_FINAL_REPROCESSAMENTO'
        AddToDic(traceId, mensagemId, conteudoMensagem, tipoNotificacao, status)
        jdata = jsonify({'mensagemId': mensagemId, 'traceId': traceId, 'conteudoMensagem': conteudoMensagem, 'tipoNotificacao': tipoNotificacao, 'Status': status})
        enviar_mensagem(jdata, rmq_dead)

    # Simulando o processamento
    time.sleep(2)
    status = 'REPROCESSADO_COM_SUCESSO'
    AddToDic(traceId, mensagemId, conteudoMensagem, tipoNotificacao, status)
    jdata = jsonify({'mensagemId': mensagemId, 'traceId': traceId, 'conteudoMensagem': conteudoMensagem,
                     'tipoNotificacao': tipoNotificacao, 'Status': status})
    enviar_mensagem(jdata, rmq_validacao)


def main():
    client = classRabbitMQ.get_instance()
    channel = client.get_channel()

    # Declaração da fila
    channel.queue_declare(queue=rmq_retry, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=rmq_retry, on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    main()
