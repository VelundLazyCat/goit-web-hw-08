import os
import sys
import pika
from producer import Contact


def main():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='phone_queue', durable=True)

    def send_sms(data):
        print('SMS sent to phone: ', data)
        return 1

    def callback(ch, method, properties, body):
        message = body.decode()
        print(f"Received User ID: <<<{message}>>> from phone_queue")
        # функція-заглушка імітуюча відправлення емейл відповідному контакту
        send_sms(Contact.objects(id=message)[0].phone)
        # зміна поля значення done
        contact = Contact.objects(id=message)[0]
        contact.update(done=True)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='phone_queue', on_message_callback=callback)

    print('Waiting for messages. To exit press CTRL+C >>>')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
