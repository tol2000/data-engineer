#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function
import aerospike
from aerospike import predicates
from aerospike import exception
import logging
import time

logging.getLogger().setLevel(logging.WARNING)

maxLoops = 3000

print("Домашнее задание по лекции 17")
print("Реализовал требуемые функции")
print("Позволил себе для наглядности")
print("  - немного поменять соответствие атрибутов (phone=id+1, ltv=id+2)")
print("  - увеличить количество циклов до", maxLoops)
print("Индекс строится в рантайме")
print("Дополнительно ввел некоторые функции и проверки, чтобы убедиться, что все работает корректно")

aeroHost = ("127.0.0.1", 3000)
aeroSet = "customers"

# Инициализация подключения
config = {
    'hosts': [ aeroHost ]
}
try:
    client = aerospike.client(config).connect()
except:
    import sys
    print("failed to connect to the cluster with", config['hosts'])
    sys.exit(1)

# Выбор пространства имен и его предварительная инициализация 

# Поскольку предупреждали, что запуск будет производиться на чистом аэроспайке:
#   - мы не знаем, какой там неймспейс, в итоге берем первый попавшийся :)
#   - на ходу создаем вторичный индекс по бину телефона, игнорируя исключение, если такой уже есть

aeroNameSpace = client.info_node("namespaces", aeroHost, None).split("\n")[0].split("\t")[1]
print("Cluster's current namespace is",aeroNameSpace)

try:
    print("Creating indexes...")
    client.index_integer_create(aeroNameSpace, aeroSet, "phone_number", aeroNameSpace+"_"+aeroSet+"_phone_number_idx")
except exception.IndexFoundError:
    print("Needed indexes already exists")
else:
    print("Indexes created")

print("Secondary indexes:")
print(client.info('sindex/test'))


def add_customer(customer_id, phone_number:str, lifetime_value):
    try:
        client.put( (aeroNameSpace, aeroSet, customer_id), {
            "phone_number": phone_number,
            "lifetime_value": lifetime_value
        })
    except Exception as e:
        logger.error("error: {0}".format(e))

# Дополнительная функция нахождения любого бина customers по главному ключу
def get_bin_by_id(customer_id, binName):
    try:
        (key, metadata, record) = client.get((aeroNameSpace, aeroSet, customer_id))
        return record.get(binName)
    except Exception as e:
        logger.error('Requested non-existent customer ' + str(customer_id))

def get_ltv_by_id(customer_id):
    try:
        (key, metadata, record) = client.get((aeroNameSpace, aeroSet, customer_id))
        return record.get("lifetime_value")
    except Exception as e:
        logger.error('Requested non-existent customer ' + str(customer_id))

def get_ltv_by_phone(phone_number):
    try:
        # Создаем запрос для поиска телефона
        # Оказывается, однажды созданный экземпляр Query при последующих вызовах других select и where никак на это не реагирует.
        # Приходится создавать новый, освобождая существующий.
        aeroQuery = client.query(aeroNameSpace, aeroSet)
        aeroQuery.select( "lifetime_value" )
        aeroQuery.where( predicates.equals("phone_number", phone_number) )
        records = aeroQuery.results( {'total_timeout':2000})
        (dummy1, dummy2, pDict) = records[0] # first found string
        return pDict.get( "lifetime_value" )
    except Exception as e:

        logging.error('Requested phone number is not found ' + str(phone_number))

# Проверяем созданные функции, заодно замеряем время поиска по ключу и индексу

print("Execution begins...")

startTime = time.time()
for i in range(0, maxLoops):
    add_customer( i, i + 1, i + 2 )
print("Insert time by id: ", (time.time()-startTime))

startTime = time.time()
for i in range( 0, maxLoops ):
    assert (i + 2 == get_ltv_by_id(i)), "No LTV by ID " + str(i)
print("Select time by PK(customer_id): ", (time.time()-startTime))

startTime = time.time()
for i in range( 0, maxLoops ):
    assert (i + 2 == get_ltv_by_phone(i+1)), "No LTV by phone " + str(i)
print("Select time by secondary index (phone_number): ", (time.time()-startTime))

print ("All is OK! :)")

# Попутно сравниваем визуально номера телефонов и ltv по id, затем по этим же номерам телефонов выбираем снова ltv

print("Some additional visual check and compare")
for i in range (maxLoops-5, maxLoops):
    cPhone = get_bin_by_id( i, "phone_number" )
    print( "Customer", i, ", Phone", cPhone, ", ltv", get_ltv_by_id(i), "ltv by phone", get_ltv_by_phone(cPhone) )

# Закрываем соединение
client.close()
