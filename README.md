# Инструкция по запуску
## Сборка образов
docker-compose build --no-cache
## Запуск сервисов в фоновом режиме
docker-compose up -d
## Создание Kafka-топика
docker-compose exec kafka kafka-topics --create ^  
--topic csv-topic ^  
--bootstrap-server localhost:9092 ^  
--partitions 1 ^  
--replication-factor 1
## Список топиков
docker-compose exec kafka kafka-topics --list ^  
--bootstrap-server localhost:9092
## Запуск Flink-job в фоне
docker-compose exec -d jobmanager flink run -py /opt/flink/jobs/job.py
## Запуск Producer для отправки данных
docker-compose --profile manual run --rm producer
## Flink UI
http://localhost:8081/
## Kafka UI
http://localhost:8080/
## Postgres
docker-compose exec postgres psql -U admin -d sales_warehouse  

SELECT 'dim_customer' as table_name, COUNT(*) as row_count FROM dim_customer  
UNION ALL  
SELECT 'dim_seller', COUNT(*) FROM dim_seller  
UNION ALL  
SELECT 'dim_store', COUNT(*) FROM dim_store  
UNION ALL  
SELECT 'dim_supplier', COUNT(*) FROM dim_supplier  
UNION ALL  
SELECT 'dim_product', COUNT(*) FROM dim_product  
UNION ALL  
SELECT 'dim_pet', COUNT(*) FROM dim_pet  
UNION ALL  
SELECT 'dim_date', COUNT(*) FROM dim_date  
UNION ALL  
SELECT 'fact_sales', COUNT(*) FROM fact_sales;  
## Остановка и удаление контейнеров
docker-compose down -v  
docker system prune -a

# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

Одним из самых популярных фреймворков для работы со streaming processing является Apache Flink. Apache Flink - мощный фреймворк, который предлагает широкий набор функциональности для простого написания streaming processing.

Что необходимо сделать? 

Необходимо реализовать потоковую обработку данных с помощью Flink, который читает топик Kafka, трансформирует данные в режиме streaming в модель звезда и пишет результат в PostgreSQL. Данные в Kafka-топиках хранятся в формате json. Данные в топик kafka нужно отправлять самостоятельно, эмулируя источник данных.

Какие данные отправляются в Kafka?
 - Каждое сообщение в Kafka-топике - это строчка из csv файлов, преобразованная в формат json.

Какие данные отправляются в PostgreSQL?
 - Трансформированные данные в модель данных звезда.

![Лабораторная работа №3](https://github.com/user-attachments/assets/d3c1544d-3fe6-4c15-b673-9aa5d27dbd76)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Flink (рекомендую установку через Docker).
5. Устанавливаете Apache Kafka (рекомендую установку через Docker).
6. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
7. Реализуете приложение, которое каждую строчку из исходных csv-файлов преобразует в json и отправляет в виде сообщения в Kafka-топик.
8. Реализуете приложение на Flink, которое читает Kafka-топик, преобразует данные в модель звезда и сохраняет в PostgreSQL в режиме streaming.
9. Проверяете конечные данные в PostgreSQL.
10. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Flink, Kafka и запуском приложения, которое из файлов mock_data(*).csv создает сообщения json в Kafka.
3. Инструкция, как запускать Flink-джобу и приложение для отправки данных в Kafka для проверки лабораторной работы.
4. Код Apache Flink для трансформации данных в режиме streaming.
