# Kafka Unit 1: продюсер и два консьюмера

Учебное приложение для Apache Kafka. Продюсер раз в 200 мс отправляет сообщение в топик `kafka_topic`. Два консьюмера читают из топика одни и те же сообщения, но по-разному:

- `SingleMessageConsumer` читает по одному сообщению, смещения коммитятся автоматически;
- `BatchMessageConsumer` читает пачками не меньше чем по 10 сообщений и коммитит смещения вручную, один раз после каждой пачки.

Сообщения передаются в формате JSON.

**Стек:** Java 21, Spring Boot 4.0.3, Gradle 9.3.1, `kafka-clients` 4.1.1, Jackson 3, Lombok. Кластер Kafka работает в Docker: ZooKeeper и 3 брокера `confluentinc/cp-kafka:7.0.1`, к ним Schema Registry и Kafka UI.

## Как работает приложение

```
                                            ┌─► SingleMessageConsumer (группа single-message-group)
MessageProducer ─► kafka_topic ─────────────┤
                   (3 партиции, 2 реплики)  └─► BatchMessageConsumer  (группа batch-message-group)
```

**Запуск и остановка.** После старта Spring Boot вызывает `KafkaLoop.run()`. `KafkaLoop` запускает продюсер и оба консьюмера, каждый в своём потоке: `producer`, `single-consumer` и `batch-consumer`. Каждый из них работает в цикле, пока его не остановят. При остановке приложения (Ctrl+C или `docker compose stop`) Spring вызывает метод `KafkaLoop` с аннотацией `@PreDestroy`. Этот метод просит все три компонента остановиться и ждёт, пока их потоки завершатся.

**Клиенты Kafka закрываются как io-потоки.** Продюсер и консьюмеры создают свой `KafkaProducer` или `KafkaConsumer` в начале `run()` в блоке try-with-resources, поэтому клиент закрывается при любом выходе из цикла. При закрытии продюсер дожидается отправки оставшихся сообщений, а консьюмер выходит из группы. `KafkaConsumer` не потокобезопасен, поэтому каждый консьюмер работает со своим клиентом только из своего потока. Из потока остановки вызывается только `wakeup()`, который прерывает ожидание в `poll()`.

**Разные группы.** У каждого типа консьюмера своя группа (`group.id`), а каждая группа получает все сообщения топика. Поэтому каждое сообщение обрабатывают оба консьюмера. Когда запущено два экземпляра приложения, в каждой группе два участника. Kafka делит между ними 3 партиции (2 + 1), так что внутри группы каждое сообщение обрабатывает только один экземпляр.

**Сериализация.** Сообщение представлено record-классом `Message`. Перед отправкой `MessageSerializer` превращает его в JSON, а в консьюмерах `MessageDeserializer` превращает JSON обратно в `Message`. Если JSON прочитать не удалось, десериализатор пишет ошибку в лог и возвращает `null`, а не бросает исключение. Консьюмер пропускает такое сообщение и продолжает работу. С исключением `poll()` падал бы на этом сообщении снова и снова.

**Гарантии доставки.** Продюсер настроен на гарантию At Least Once:

- `acks=all`: брокер подтверждает запись, только когда её получили все синхронные реплики (ISR);
- `retries=3`, `retry.backoff.ms=500`: при временной ошибке, например при смене лидера партиции, продюсер повторяет отправку до 3 раз с паузой 500 мс.

Консьюмеры коммитят смещения только после обработки сообщений. `BatchMessageConsumer` вызывает `commitSync()` после того, как обработал всю пачку. `SingleMessageConsumer` при автокоммите фиксирует смещения сообщений, которые вернул предыдущий вызов `poll()`, а они к этому моменту уже обработаны.

**Ошибки.** Ошибки не останавливают консьюмеры: исключение при чтении (`poll()`, `commitSync()`) записывается в лог, и цикл продолжается. `BatchMessageConsumer` ловит ошибку обработки каждого сообщения отдельно, чтобы она не мешала остальным сообщениям пачки. Ошибки продюсера тоже попадают в лог. Ошибки, возникшие до отправки (например, при сериализации), ловятся вокруг `send()`, а ошибки от брокера приходят в callback.

## Классы

Все классы находятся в пакете `com.dmitrykoryanov.kafka.kafkaunit1`.

| Класс | Назначение |
|---|---|
| `KafkaUnit1Application` | Точка входа Spring Boot. |
| `KafkaLoop` | Реализует `CommandLineRunner`. После старта запускает продюсер и консьюмеры в отдельных потоках. При остановке (`@PreDestroy`) останавливает их и ждёт завершения потоков, до 10 секунд на поток. |
| `MessageProducer` | Раз в 200 мс создаёт `Message` со следующим номером, выводит его в лог и отправляет в топик. Ключ записи — `id` сообщения, поэтому сообщения распределяются по всем партициям. |
| `SingleMessageConsumer` | Читает по одному сообщению за `poll()` (`max.poll.records=1`). Смещения коммитятся автоматически (`enable.auto.commit=true`). |
| `BatchMessageConsumer` | Читает пачками. При `fetch.min.bytes=2500` брокер отвечает, только когда накопит около 10 сообщений, но ждёт не дольше `fetch.max.wait.ms=10000`, то есть 10 секунд. Пачку обрабатывает в цикле, затем один раз вызывает `commitSync()` (`enable.auto.commit=false`). |
| `KafkaUtils` | Создаёт продюсер и консьюмеры с нужными настройками. Адреса брокеров, топик и группы берёт из `application.properties`. |
| `Message` | Сообщение: `id` (UUID), `number`, `text`, `createdAt`. |
| `MessageSerializer` | Превращает `Message` в JSON. При ошибке пишет в лог и бросает `SerializationException`, и сообщение не отправляется. |
| `MessageDeserializer` | Превращает JSON в `Message`. При ошибке пишет в лог и возвращает `null`. |

## Настройки

Файл `src/main/resources/application.properties`:

| Свойство | Значение | Назначение |
|---|---|---|
| `kafka.bootstrap-servers` | `localhost:9093,localhost:9094,localhost:9095` | адреса брокеров для запуска с хоста |
| `kafka.topic` | `kafka_topic` | топик |
| `kafka.single-consumer.group-id` | `single-message-group` | группа `SingleMessageConsumer` |
| `kafka.batch-consumer.group-id` | `batch-message-group` | группа `BatchMessageConsumer` |

Любое свойство можно переопределить переменной окружения. Например, в `docker-compose.yml` приложение получает внутренние адреса брокеров через `KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092`.

## Запуск

Нужны Docker Desktop и JDK 21. Команды ниже написаны для Git Bash. В PowerShell вместо `./gradlew` используйте `gradlew.bat`. Предупреждение Docker Compose `the attribute version is obsolete` можно игнорировать.

### 1. Запустить кластер Kafka

```bash
docker compose up -d zookeeper kafka1 kafka2 kafka3 schema-registry kafka-ui
```

Брокеры запускаются за 20–30 секунд. Kafka UI доступен по адресу http://localhost:8080.

### 2. Создать топик (шаг 1 задания)

Топик создаётся один раз, через консоль внутри контейнера брокера:

```bash
docker exec kafka1 kafka-topics --create --topic kafka_topic \
  --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 2
```

Топик нужно создать до запуска приложения. Иначе брокер создаст его сам при первом обращении продюсера, но с 1 партицией и 1 репликой.

### 3. Запустить приложение в двух экземплярах

```bash
./gradlew bootJar                  # образ приложения копирует собранный jar
docker compose up -d --build app
```

Сервис `app` в `docker-compose.yml` запускается в двух экземплярах (`deploy.replicas: 2`): `kafka-unit-1-app-1` и `kafka-unit-1-app-2`. Логи обоих экземпляров:

```bash
docker compose logs -f app
```

Для отладки можно запустить один экземпляр с хоста, из IDE или командой:

```bash
java -Dstdout.encoding=UTF-8 -jar build/libs/kafka-unit-1-0.0.1-SNAPSHOT.jar
```

Флаг `-Dstdout.encoding=UTF-8` нужен, чтобы русский текст в консоли Windows не превращался в `????`.

### 4. Остановить

```bash
docker compose stop app     # только приложение
docker compose stop         # всё
```

Остановка приложения занимает до 10–15 секунд: закрытие `BatchMessageConsumer` может длиться до `fetch.max.wait.ms`, то есть до 10 секунд. Поэтому в `docker-compose.yml` задан `stop_grace_period: 15s`.

`docker compose down` удаляет контейнеры. Новые контейнеры кластера начнут с пустыми данными, и топик придётся создать заново.

## Проверка работы по заданию

Запустите кластер и приложение в двух экземплярах (раздел «Запуск», шаги 1–3) и подождите 20–30 секунд.

### Шаг 1. Топик с 3 партициями и 2 репликами

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:9092 --describe --topic kafka_topic
```

Ожидаемый результат: `PartitionCount: 3` и `ReplicationFactor: 2`, у каждой партиции по две реплики в `Replicas` и `Isr`. Номера лидеров могут отличаться:

```
Topic: kafka_topic	TopicId: ...	PartitionCount: 3	ReplicationFactor: 2	Configs:
	Topic: kafka_topic	Partition: 0	Leader: 3	Replicas: 3,2	Isr: 3,2
	Topic: kafka_topic	Partition: 1	Leader: 1	Replicas: 1,3	Isr: 3,1
	Topic: kafka_topic	Partition: 2	Leader: 2	Replicas: 2,1	Isr: 2,1
```

### Шаг 2. Один продюсер, два консьюмера, два экземпляра приложения

**Запущено два экземпляра.** Команда `docker compose ps app` показывает два контейнера в состоянии `Up`.

**Продюсер отправляет сообщения.** В выводе `docker compose logs -f app` от обоих экземпляров идут строки `Отправка: Message[...]`. У каждого экземпляра свой продюсер со своей нумерацией, поэтому номера сообщений повторяются. Различать сообщения нужно по `id`.

**Оба консьюмера читают одни и те же сообщения.** Возьмите `id` любого сообщения из строки `Отправка` и найдите его в логах:

```bash
docker compose logs app | grep <id>
```

Должно найтись три строки: `Отправка` от `MessageProducer`, `Получено` от `SingleMessageConsumer` и `Получено` от `BatchMessageConsumer`. Строки могут прийти от разных экземпляров.

**У каждого типа консьюмера своя группа, и экземпляры делят партиции.**

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:9092 --describe --group single-message-group --members
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:9092 --describe --group batch-message-group --members
```

В каждой группе должно быть два участника с разными `HOST` (по одному на экземпляр): у одного 2 партиции, у другого 1.

**Настройки консьюмеров.** При старте Kafka выводит в лог настройки каждого клиента:

```bash
docker compose logs app | grep -E "group.id =|max.poll.records =|enable.auto.commit =|fetch.min.bytes =|fetch.max.wait.ms ="
```

Для каждого экземпляра выводится по блоку на консьюмер:

```
app-2  | 	enable.auto.commit = true
app-2  | 	fetch.max.wait.ms = 500
app-2  | 	fetch.min.bytes = 1
app-2  | 	group.id = single-message-group
app-2  | 	max.poll.records = 1
app-1  | 	enable.auto.commit = false
app-1  | 	fetch.max.wait.ms = 10000
app-1  | 	fetch.min.bytes = 2500
app-1  | 	group.id = batch-message-group
app-1  | 	max.poll.records = 500
```

- Блок `single-message-group`: `max.poll.records = 1` (одно сообщение за `poll()`) и `enable.auto.commit = true` (автоматический коммит).
- Блок `batch-message-group`: `enable.auto.commit = false` (ручной коммит), `fetch.min.bytes = 2500` и `fetch.max.wait.ms = 10000` (пачки).

**`SingleMessageConsumer` обрабатывает по одному сообщению.** На каждое сообщение в логе одна строка `Получено` от `SingleMessageConsumer`.

**`BatchMessageConsumer` получает пачки не меньше 10 сообщений.**

```bash
docker compose logs app | grep "пачка из"
```

Ожидаемый результат — строки `Получена пачка из 11 сообщений`, изредка с бо́льшим числом. За каждой такой строкой идут строки `Получено` для каждого сообщения пачки. Если продюсер работает медленнее, пачка может оказаться меньше 10 сообщений: брокер ждёт не дольше `fetch.max.wait.ms`, то есть 10 секунд.

**Смещения коммитятся.** В выводе `kafka-consumer-groups ... --describe --group <группа>` (без `--members`) значение `CURRENT-OFFSET` растёт у обеих групп. У `batch-message-group` автокоммит выключен, значит, смещения фиксирует вызов `commitSync()`.

**Ошибки не останавливают консьюмеры.** Отправьте в топик сообщение, которое не является JSON, и найдите его в логах:

```bash
echo "not a json" | docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:9092 --topic kafka_topic
docker compose logs --since 1m app | grep -E "not a json|Пропускаем"
```

Ожидаемый результат — две пары строк, от `SingleMessageConsumer` и от `BatchMessageConsumer` (возможно, из разных экземпляров):

```
app-1  | ... ERROR ... [single-consumer] c.d.k.kafkaunit1.MessageDeserializer     : Не удалось десериализовать сообщение 'not a json': Unrecognized token 'not': ...
app-1  | ... WARN  ... [single-consumer] c.d.k.kafkaunit1.SingleMessageConsumer   : Пропускаем сообщение, которое не удалось прочитать: партиция 1, оффсет 538
app-2  | ... ERROR ... [ batch-consumer] c.d.k.kafkaunit1.MessageDeserializer     : Не удалось десериализовать сообщение 'not a json': Unrecognized token 'not': ...
app-2  | ... WARN  ... [ batch-consumer] c.d.k.kafkaunit1.BatchMessageConsumer    : Пропускаем сообщение, которое не удалось прочитать: партиция 1, оффсет 538
```

После этих строк в `docker compose logs -f app` продолжают появляться новые строки `Получено`.

### Шаг 3. Сериализация и десериализация

**Сообщения хранятся в топике в формате JSON.** Посмотреть их можно в Kafka UI (Topics → `kafka_topic` → Messages) или консольным консьюмером:

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:9092 --topic kafka_topic --from-beginning --max-messages 3
```

```
{"id":"fe21728d-5124-490f-a514-337b8f5e7192","number":6,"text":"Сообщение 6","createdAt":"2026-10-19T13:53:53.433861900Z"}
...
```

**Сообщения выводятся в лог.** Отправленное сообщение видно в строке `Отправка: Message[...]`, полученное — в строке `Получено: Message[...]`.

**Ошибки десериализации выводятся в лог.** Как это проверить, описано в конце шага 2 («Ошибки не останавливают консьюмеры»). С текущим классом `Message` ошибка сериализации не возникает, поэтому её обработку можно проверить только по коду `MessageSerializer` и `MessageProducer.send()`.

### Шаг 4. Гарантии доставки

```bash
docker compose logs app | grep -E "acks =|retries ="
```

Ожидаемый результат: `acks = -1` (так в логе выглядит `all`) и `retries = 3` у продюсера каждого экземпляра.

### Штатная остановка

```bash
docker compose stop app
```

В логах обоих экземпляров появляется строка `Останавливаем продюсер и консьюмеры`, после неё продюсер и консьюмеры закрываются: `App info kafka.producer ... unregistered` и `App info kafka.consumer ... unregistered`. Контейнеры завершаются с кодом 143 (128 + 15, остановка по сигналу SIGTERM), это нормально.
