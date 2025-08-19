import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import logging
import os
import socket

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_generator')

fake = Faker('ru_RU')


def wait_for_kafka(kafka_host, kafka_port, max_retries=30, retry_delay=5):
    """Ожидание доступности Kafka"""
    logger.info(f"Ожидаем доступность Kafka на {kafka_host}:{kafka_port}...")

    for attempt in range(max_retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((kafka_host, kafka_port))
            sock.close()

            if result == 0:
                logger.info(f"Kafka доступен на {kafka_host}:{kafka_port}")
                return True
            else:
                logger.warning(f"Попытка {attempt + 1}/{max_retries}: Kafka еще не доступен")

        except Exception as e:
            logger.warning(f"Попытка {attempt + 1}/{max_retries}: Ошибка подключения - {e}")

        time.sleep(retry_delay)

    logger.error(f"Kafka не стал доступен после {max_retries} попыток")
    return False


class DataGenerator:
    def __init__(self, kafka_broker):
        self.kafka_broker = kafka_broker
        self.producer = None
        self.products = self._generate_products(50)
        self.warehouses = ['Москва', 'Волгоград', 'Санкт-Петербург', 'Казань', 'Новосибирск', 'Екатеринбург']

    def initialize_producer(self):
        """Инициализация продюсера"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_broker],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks=1,
                retries=3,
                request_timeout_ms=10000,
                api_version=(2, 8, 0)
            )
            logger.info(f"Продюсер инициализирован для {self.kafka_broker}")
            return True
        except Exception as e:
            logger.error(f"Ошибка инициализации продюсера: {e}")
            return False

    def _generate_products(self, count):
        return [{
            'id': i + 1,
            'name': f"{fake.word()} {fake.word()}",
            'category': random.choice(['Электроника', 'Одежда', 'Продукты', 'Книги', 'Бытовая техника']),
            'price': round(random.uniform(100, 10000), 2),
            'base_cost': round(random.uniform(50, 5000), 2)
        } for i in range(count)]

    def generate_sale(self):
        product = random.choice(self.products)
        sale = {
            'event_id': fake.uuid4(),
            'event_type': 'sale',
            'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': product['id'],
            'product_name': product['name'],
            'category': product['category'],
            'quantity': random.randint(1, 5),
            'price': float(product['price']),
            'discount': float(round(random.uniform(0, 0.3), 2)),
            'total': float(round(product['price'] * (1 - random.uniform(0, 0.3)), 2)),
            'store_id': random.randint(1, 10),
            'cashier_id': random.randint(1, 20),
            'customer_id': fake.uuid4(),
        }
        return sale

    def generate_stock_movement(self):
        product = random.choice(self.products)
        movement = {
            'event_id': fake.uuid4(),
            'event_type': 'stock_movement',
            'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': product['id'],
            'product_name': product['name'],
            'category': product['category'],
            'warehouse': random.choice(self.warehouses),
            'quantity': random.randint(1, 100),
            'movement_type': random.choice(['supply', 'relocation', 'write_off']),
            'source': fake.company(),
            'responsible': fake.name()
        }
        return movement

    def run(self):
        logger.info("Генератор данных запущен")

        # Парсим хост и порт из строки подключения
        kafka_host, kafka_port_str = self.kafka_broker.split(':')
        kafka_port = int(kafka_port_str)

        # Ожидаем доступность Kafka
        if not wait_for_kafka(kafka_host, kafka_port):
            logger.error("Не удалось подключиться к Kafka, завершаем работу")
            return

        # Инициализируем продюсер
        if not self.initialize_producer():
            logger.error("Не удалось инициализировать продюсер")
            return

        logger.info("Начинаем генерацию данных...")

        message_count = 0
        while True:
            try:
                if random.random() < 0.7:
                    event = self.generate_sale()
                    topic = 'sales'
                else:
                    event = self.generate_stock_movement()
                    topic = 'warehouse'

                self.producer.send(topic, event)
                message_count += 1

                if message_count % 10 == 0:
                    logger.info(f"Отправлено {message_count} сообщений")

                time.sleep(random.uniform(0.5, 1.5))

            except Exception as e:
                logger.error(f"Ошибка при отправке: {e}")
                time.sleep(5)
                # Пытаемся переинициализировать продюсер
                try:
                    if self.producer:
                        self.producer.close()
                except:
                    pass
                self.producer = None
                self.initialize_producer()


if __name__ == "__main__":
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:29092')
    logger.info(f"Запуск генератора данных для Kafka: {kafka_broker}")

    generator = DataGenerator(kafka_broker)
    generator.run()