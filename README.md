# Базовая интеграция Kafka + ClickHouse

Данный проект демонстрирует современный подход к построению потоковых ETL-конвейеров с использованием Apache Kafka в качестве брокера сообщений и ClickHouse как высокопроизводительной колоночной СУБД для аналитики.

## Содержание

- [Цель проекта](#цель-проекта)
- [Архитектура решения](#архитектура-решения)
- [Структура данных](#структура-данных)
- [Быстрый старт](#быстрый-старт)
- [Механика работы](#механика-работы)
- [Ключевые выводы](#ключевые-выводы)

## Цель проекта

Реализовать на практике базовую интеграцию Kafka + ClickHouse с разветыванием в Docker-окружении, включая:
- Real-time потребление данных из Kafka топиков
- Автоматическую трансформацию данных с помощью Materialized Views
- Создание оптимизированных хранилищ для аналитических запросов

## Архитектура решения
Основной поток данных
```text
[Data Generator] → [Kafka Topics] → [Kafka Engine Tables] → [Materialized Views] → [Target Tables]
```
Детальная схема
```text
Python App → Kafka (sales topic) → sales_kafka (Kafka Engine) → sales_mv (MV) → sales (MergeTree)
Python App → Kafka (warehouse topic) → warehouse_kafka (Kafka Engine) → stock_movements_mv (MV) → stock_movements (MergeTree)
```
## Структура данных
Топики Kafka:
  - sales - данные о продажах (70% трафика)
  - warehouse - движения товаров на складе (30% трафика)

Таблицы с движком Kafka (приемники):
  - sales_kafka - приемник для топика sales
  - warehouse_kafka - приемник для топика warehouse

Материализованные представления для трансформации данных от таблиц приемников в целевые:
  - sales_mv  - трансформация для продаж
  - stock_movements_mv  - трансформация для движений склада

Целевые таблицы ClickHouse
  - sales - финальные данные продаж (MergeTree)
  - stock_movements - движения товаров (MergeTree)

## Быстрый старт
Предварительные требования
  - Docker
  - Docker Compose
  - 4+ Гб ОЗУ

### Запуск проекта
  ```bash
# Клонирование репозитория
git clone https://github.com/andreynetrebin/kafka_clickhouse_pipeline/
cd kafka_clickhouse_pipeline

# Запуск всех сервисов
docker-compose up -d

# Проверка статуса сервисов
docker-compose ps -a

# Просмотр логов генератора данных
docker-compose logs data-generator -f

# Просмотр логов ClickHouse
docker-compose logs clickhouse -f
  ```

## Механика работы
1. Генерация данных с интенсивностью с 0.5-1.5 секунды между сообщениями
   ```python
    # Python-приложение генерирует тестовые данные в json-формате
    # и отправляет в соответствующие топики Kafka
    producer.send('sales', sale_data)
    producer.send('warehouse', movement_data)
   ```
2. Потребление из Kafka
  ```sql
  -- Таблицы Kafka Engine подключаются к топикам
  CREATE TABLE sales_kafka (...) ENGINE = Kafka(
      kafka_broker_list = 'kafka:29092',
      kafka_topic_list = 'sales',
      kafka_group_name = 'clickhouse_sales_consumer',
      kafka_format = 'JSONEachRow',
      kafka_group_name = 'clickhouse_consumer'
  );
  ```  
3. Трансформация через материализованные представления (Materialized Views)
   ```sql
    -- MV автоматически преобразуют и переносят данные
    CREATE MATERIALIZED VIEW sales_mv TO sales AS
    SELECT 
        event_id,
        parseDateTimeBestEffort(event_time) as event_time,
        -- ... преобразования полей
    FROM sales_kafka;
   ```
4.  Хранение в оптимизированных таблицах
   ```sql
    -- Данные хранятся в MergeTree с партицированием по месяцам и сортировкой
    CREATE TABLE sales (
        -- ...
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(event_time)
    ORDER BY (event_time, product_id);
   ```

## Ключевые выводы
✅ Что успешно реализовано:
  - Рабочий ETL-конвейер в Docker-окружении
  - Real-time потребление данных из Kafka топиков
  - Автоматическая трансформация через Materialized Views
  - Готовое решение для аналитических сценариев

Идеальные сценарии для данной архитектуры:
```sql
-- Данные преимущественно append-only
-- Высокие объемы записи (50-100K+ сообщений/сек)
-- Требуется отказоустойчивость и буферизация нагрузки
-- Основное назначение - аналитика и мониторинг
```
Когда рассмотреть другие подходы:
```sql
-- Частые обновления данных (использовать CDC + батч-обработку)
-- Критичен строгий порядок событий 
-- Требуются ACID транзакции
-- Приоритет простоте над масштабируемостью
```
Данная базовая архитектура отлично подходит для 80% аналитических сценариев, обеспечивая высокую производительность и надежность при минимальной сложности реализации.

