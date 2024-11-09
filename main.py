import redis
from redis_lru import RedisLRU
from models import Author, Quote
from sys import exit

client = redis.StrictRedis(host="localhost", port=6379, password=None)
cache = RedisLRU(client)


@cache
def find_by_tag(tag):
    print('request to DataBase')
    quotes = Quote.objects(tags__iregex=tag)
    result = [q.quote for q in quotes]
    return result


@cache
def find_by_author(author):
    print('request to DataBase')
    authors = Author.objects(fullname__iregex=author)
    result = {}
    for a in authors:
        quotes = Quote.objects(author=a)
        result[a.fullname] = [q.quote for q in quotes]
    return result


if __name__ == '__main__':
    COMMANDS = {'name': find_by_author,
                'tag': find_by_tag,
                'tags': find_by_tag}

    while True:
        tag = input(
            "Enter command for search (name/tag: <word>), 'exit' for Quit\n>>> ")

        tag = tag.split(':')
        if tag[0].strip().lower() == 'exit':
            exit()
        try:
            a = COMMANDS[tag[0].strip().lower()]
            for search in tag[1].split(','):
                print(a(search.strip()))
        except:
            print('Unknown command')
