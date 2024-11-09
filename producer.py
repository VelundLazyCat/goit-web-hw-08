import pika
import connect_mongo
from faker import Faker
from random import choice
from mongoengine import Document, StringField, BooleanField


NUMBER_CONTACTS = 10
fake_data = Faker('uk-UA')


class Contact(Document):
    fullname = StringField(max_length=50, required=True)
    city = StringField(max_length=50)
    address = StringField(max_length=100)
    email = StringField(max_length=50, required=True)
    phone = StringField(max_length=50, required=True)
    choice = StringField(max_length=10)
    done = BooleanField(default=False)
    meta = {"collection": "contacts"}


def seed(nums=NUMBER_CONTACTS):
    for _ in range(nums):
        contact = Contact(fullname=fake_data.name(), city=fake_data.city(),
                          address=fake_data.address(), email=fake_data.email(),
                          phone=fake_data.phone_number(), choice=choice(['email', 'phone']))
        contact.save()


def get_contacts_id():
    result = []
    contacts = Contact.objects()
    for con in contacts:
        if not con.done:
            result.append(con.id)
    return result


def create_tasks(data):
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.exchange_declare(exchange='Home Work', exchange_type='direct')
    channel.queue_declare(queue='email_queue', durable=True)
    channel.queue_declare(queue='phone_queue', durable=True)
    channel.queue_bind(exchange='Home Work', queue='email_queue')
    channel.queue_bind(exchange='Home Work', queue='phone_queue')

    for i, d in enumerate(data):
        contact = Contact.objects(id=str(d))[0]
        if contact.choice == 'email':
            channel.basic_publish(exchange='Home Work',
                                  routing_key='email_queue', body=str(d).encode())
        elif contact.choice == 'phone':
            channel.basic_publish(exchange='Home Work',
                                  routing_key='phone_queue', body=str(d).encode())
        print(f" Number: {i}, method: {contact.choice} sent Id is: {d}")

    connection.close()


if __name__ == '__main__':
    # Розкоментувати для наповнення нової бази контактів
    # seed()
    contacts = get_contacts_id()
    create_tasks(contacts)
