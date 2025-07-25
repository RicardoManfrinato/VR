from flask import Flask, request, jsonify
from cRabbitMQ import classRabbitMQ
import pika
import json
import time, uuid

app = Flask(__name__)

dictMsg = []
rmq_fila = 'fila.notificacao.entrada.Ricardo.Granado.Manfrinato'
rmq_retry = 'fila.notificacao.retry.Ricardo.Granado.Manfrinato'
rmq_validacao = 'fila.notificacao.validacao.Ricardo.Granado.Manfrinato'
rmq_dead = 'fila.notificacao.dlq.Ricardo.Granado.Manfrinato'


def AddToDic(traceid, mensagemid, conteudomensagem, tiponotificacao, status):
    dictMsg.append({"traceId": traceid, "mensagemId": mensagemid, "conteudomensagem": conteudomensagem, "tiponotificacao": tiponotificacao, "status": status})

def enviar_mensagem(message, fila):
    client = classRabbitMQ.get_instance()
    channel = client.get_channel()

    # Declare a queue
    channel.queue_declare(queue=rmq_fila, durable=True)

    # Publish the message
    channel.basic_publish(
        exchange='',
        routing_key=rmq_fila,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
        )
    )


@app.route('/api/notificar', methods=['POST'])
def api_notificar():
    data = request.json

    #validação
    mensagem = json.loads(data)

    traceId = uuid.uuid1()
    mensagemId = mensagem["mensagemId"]
    conteudoMensagem = mensagem["conteudoMensagem"]
    tipoNotificacao = mensagem["tipoNotificacao"]
    status = "RECEBIDO"

    AddToDic(traceId, mensagemId, conteudoMensagem, tipoNotificacao, status)
    status = None

    if mensagemId is None:
        # Cria um messageId
        timestamp = time.time()
        mensagemId = int(timestamp * 1000000) % 100000

    if conteudoMensagem is None:
        status = "conteudoMensagem inexistente"

    if tipoNotificacao is None:
        #erro
        status = "tipoNotificacao inexistente"

    valid_types = ["EMAIL", "SMS", "PUSH"]
    if tipoNotificacao not in valid_types:
        #erro
        status = "tipoNotificacao invalido"

    #Como nao havia procedimento em caso de erro de validação,
    # adicionei uma nova linha no Dict indicando o erro na coluna Status
    if status is not None:
        AddToDic(traceId, mensagemId, conteudoMensagem, tipoNotificacao, status)

    jdata = jsonify({'mensagemId': mensagemId, 'traceId': traceId, 'conteudoMensagem': conteudoMensagem, 'tipoNotificacao': tipoNotificacao, 'Status': 'Accepted'})
    enviar_mensagem(jdata, rmq_fila)

    return jsonify({'mensagemId': mensagemId, 'traceId': traceId, 'Status': 'Accepted'}), 202


if __name__ == '__main__':
    app.run(debug=True)
