from curses import has_key
import traceback
from flask import Flask, request, jsonify
import pika
import json

app = Flask(__name__)

client = app.test_client()

#Объявить свое исключения для обработки завершения чтения из очереди
class EndOfReading(Exception):
    def __init__(self, text):
        self.txt = text

#Отправка сообщений в очередь
@app.route('/exchange', methods=['POST'])
def sendMessage():
    requestMessage = getMessage(request)
    messageList = getMessageList(requestMessage)
    if len(messageList) == 0:
        return 'Нет сообщений, которые можно передать', 400

    connection = getConnection('localhost')
    
    channel = getChannel(connection)
    exchange = ''
    queueName = 'test'
    queue = getQueue(channel, queueName)
    
    for message in messageList:
        publishMessage(channel, exchange, queueName, message)
    
    closeConnection(connection)
    
    return 'OK', 200
    
#Получение сообщений из очереди
@app.route('/exchange/<int:messagesLimitNumber>', methods=['GET'])
def getMessages(messagesLimitNumber):
    
    global result, downloadedMessagesNumber, messagesLimitNumber_
    def on_message(channel, method, properties, body):
        global downloadedMessagesNumber, messagesLimitNumber_, result
        putMessageToResult(result, body)
        acknowledgeMessage(channel, method.delivery_tag)
        
        downloadedMessagesNumber = downloadedMessagesNumber + 1
        if downloadedMessagesNumber >= messagesLimitNumber_:
            raise EndOfReading("Чтение из очереди завершено")
        
    result                      = []
    downloadedMessagesNumber    = 0
    messagesLimitNumber_        = messagesLimitNumber
    if messagesLimitNumber_ == 0:
        return 'Nothing is taken'

    connection      = getConnection('localhost')
    channel         = getChannel(connection)
    queue           = getQueue(channel, 'test')
    messagesNumber  = getMessagesNumber(queue)
    if messagesNumber == 0:
        closeConnection(connection)
        return 'The queue is empty'

    if messagesNumber < messagesLimitNumber_:
        messagesLimitNumber_ = messagesNumber

    channel.basic_consume(queue='test', on_message_callback=on_message)
    
    try:
        channel.start_consuming()
    except EndOfReading:
        channel.stop_consuming()
        closeConnection(connection)
        result_ = json.dumps(result, ensure_ascii=False,)
        return result_
    except Exception:
        message = traceback.print_exc()
        channel.stop_consuming()
        return {message}, 500

#Служебные методы
def getConnection(host):
    parameters = pika.ConnectionParameters(host=host)
    connection = pika.BlockingConnection(parameters)
    return connection

def getChannel(connection):
    channel = connection.channel()
    return channel

def getQueue(channel, queueName):
    queue = channel.queue_declare(queue=queueName)
    return queue

def getMessagesNumber(queue):
    messagesNumber = queue.method.message_count
    return messagesNumber

def publishMessage(channel, exchange, routing_key, message):
    body = json.dumps(message, ensure_ascii=False)
    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)

def putMessageToResult(result, bodyFromMQ):
    body_str = bodyFromMQ.decode("utf-8")[:4000]
    try:
        newElement = json.loads(body_str)
    except json.JSONDecodeError:
        newElement = body_str   
    result.append(newElement)

def acknowledgeMessage(channel, delivery_tag):
    channel.basic_ack(delivery_tag=delivery_tag)

def closeConnection(connection):
    connection.close()

def getMessageList(requestMessage):
    # Сюда может прилетать либо одно, либо несколько сообщений.
    #
    # Одно сообщение можно передать следующим образом:
    # {"message": "Какое-то сообщение"}
    #
    # Множество сообщений можно передать следующим образом:
    # [
    #   {
    #       "messageID": 1, 
    #       "message": "Какое-то сообщение №1"
    #   },
    #   {
    #       "messageID": 2, 
    #       "message": "Какое-то сообщение №2"
    #   }
    #  ]
    #

    messagesList = []
    if isinstance(requestMessage, list):
        for element in requestMessage:
            messageIsParceable = isinstance(element, dict) and 'message' in element and 'messageID' in element
            if messageIsParceable:
                messagesList.append(element['message'])
        
    elif isinstance(requestMessage, dict) and 'message' in requestMessage:
        messagesList.append(requestMessage['message'])  

    else:
        messagesList.append(requestMessage)

    return messagesList 

def getMessage(request):
    requestMessage = request.get_json()
    if requestMessage == None:
        data = request.get_data()
        requestMessage = data.decode("utf-8")[:4000]
    
    return requestMessage

if __name__ == '__main__':
    app.run()