# Домашнее задание 2: RPC Schema Registry

## Обязательно напишите ФИО в свой PR!!!

## Контекст задания

В распределённых системах сервисы общаются друг с другом по сети, и их API описываются формальными схемами данных (например, Apache Thrift IDL, Protocol Buffers, Avro и т.д.). Когда сервис развивается и его схема меняется, возникает серьёзная проблема: **старые клиенты не должны сломаться при обновлении сервера, и наоборот**.

Это свойство называется **backward compatibility (обратная совместимость)**. Чтобы контролировать эволюцию схем во всей организации, используют специальный сервис — **Schema Registry** (реестр схем). Именно такой сервис вы реализуете в этом задании.

Прообразы реальных систем: Confluent Schema Registry (для Kafka/Avro), Apicurio Registry, AWS Glue Schema Registry.

---

## Цель задания

- Познакомиться с **gRPC** как транспортным протоколом для межсервисного взаимодействия.
- Разобраться с **Protocol Buffers** как языком описания API.
- Реализовать нетривиальную сервисную логику с **версионированием** и **проверкой совместимости**.
- Упаковать решение в **Docker-контейнер** — стандартная практика в распределённых системах.

---

## Что дано

В репозитории уже присутствуют:

```
| proto/
|-- schema_registry.proto
| requirements.txt
| Dockerfile
```

**Вам нужно реализовать сервер в файле `src/server.py`.**

---

## Модель данных

Система оперирует упрощённой Thrift-подобной моделью схем.

### Типы полей (`FieldType`)

| Константа              | Значение | Описание              |
|------------------------|----------|-----------------------|
| `FIELD_TYPE_UNSPECIFIED` | 0      | Не указан (запрещён)  |
| `FIELD_TYPE_I32`         | 1      | 32-битное целое       |
| `FIELD_TYPE_I64`         | 2      | 64-битное целое       |
| `FIELD_TYPE_STRING`      | 3      | Строка                |
| `FIELD_TYPE_BOOL`        | 4      | Булев                 |
| `FIELD_TYPE_DOUBLE`      | 5      | Число с плавающей точкой |

### Поле (`Field`)

```
// Поле Thrift-сущности
message Field {
  int32 id = 1; // идентификатор
  string name = 2; // символьное имя
  FieldType type = 3; // тип
  bool required = 4; // флажок required/optional
}
```

> **Важно:** идентификатором поля является `id`, а не `name`. Это намеренно отражает логику Thrift/Protobuf, где имена могут меняться, а числовые id фиксируют wire-формат.

### Структура (`Struct`)

```
// Thrift-сущность
message Struct {
  string name = 1; // неизменяемое символьное имя
  repeated Field fields = 2; // поля сущности
}
```

### Схема (`Schema`)

```
// Thrift-схема
message Schema {
  repeated Struct structs = 1; // сущности схемы
}
```

---

## gRPC API

Файл спецификации: [proto/schema_registry.proto](proto/schema_registry.proto)

Сервис предоставляет четыре RPC-метода:

### 1. `RegisterSchema`

```protobuf
rpc RegisterSchema(RegisterSchemaRequest) returns (RegisterSchemaResponse);
```

Регистрирует новую версию схемы для указанного сервиса.

**Запрос:**
```
// Запрос на регистрацию Thrift-схемы для сервиса
message RegisterSchemaRequest {
  string service_name = 1; // ключ сервиса (ключ схемы)
  Schema schema = 2; // схема
}
```

**Ответ:**
```
// Ответ на регистрацию Thrift-схемы для сервиса
message RegisterSchemaResponse {
  bool accepted = 1; // принято ли обновление схемы
  int32 version = 2; // присвоенная версия (текущий, если не принята; новый, если принята)
  repeated CompatibilityIssue issues = 3; // список проблем совместимости (пустой при успехе)
}
```

**Поведение:**
- Если схем для данного `service_name` ещё нет — принять, присвоить версию `1`.
- Иначе — сравнить с последней зарегистрированной версией по правилам совместимости.
- Если нарушений нет — сохранить, вернуть следующий номер версии.
- Если есть нарушения — **не сохранять**, вернуть `accepted=false`, текущую версию и список нарушений.

---

### 2. `CheckCompatibility`

```protobuf
rpc CheckCompatibility(CheckCompatibilityRequest) returns (CheckCompatibilityResponse);
```

Проверяет совместимость кандидата-схемы с конкретной сохранённой версией — **без сохранения**.

**Запрос:**
```
// Запрос на проверку совместимости новой схемы со старой
message CheckCompatibilityRequest {
  string service_name = 1; // ключ сервиса (ключ схемы)
  int32 base_version = 2; // номер версии, с которой произвести сравнение
  Schema candidate_schema = 3; // новая схема, которую надо проверить на совместимость с base_version
}
```

**Ответ:**
```
// Ответ на проверку совместимости новой схемы со старой
message CheckCompatibilityResponse {
  bool compatible = 1; // все ли окей по совместимости
  repeated CompatibilityIssue issues = 2; // список проблем совместимости
}
```

**Ошибки gRPC:**
- `NOT_FOUND` — сервис не найден.
- `INVALID_ARGUMENT` — `base_version` не существует или `service_name` пуст.

---

### 3. `GetSchema`

```protobuf
rpc GetSchema(GetSchemaRequest) returns (GetSchemaResponse);
```

Возвращает схему по версии.

**Запрос:**
```
// Запрос на получение схемы по версии
message GetSchemaRequest {
  string service_name = 1; // ключ сервиса (ключ схемы)
  int32 version = 2; // номер версии схемы
}
```

**Ответ:**
```
// Ответ на получение схемы по версии
message GetSchemaResponse {
  int32 version = 1; // номер версии схемы
  Schema schema = 2; // схема
}
```

**Ошибки:** `NOT_FOUND`, `INVALID_ARGUMENT`.

---

### 4. `GetLatestVersion`

```protobuf
rpc GetLatestVersion(GetLatestVersionRequest) returns (GetLatestVersionResponse);
```

Возвращает номер последней зарегистрированной версии.

**Запрос / Ответ:**
```
// Запрос на получение номера последней версии схемы
message GetLatestVersionRequest {
  string service_name = 1; // ключ сервиса (ключ схемы)
}

// Ответ на получение номера последний версии схемы
message GetLatestVersionResponse {
  int32 version = 1; // номер последней версии схемы
}
```

**Ошибки:** `NOT_FOUND`, `INVALID_ARGUMENT`.

---

## Правила совместимости

При регистрации новой версии `vN` относительно предыдущей `vN-1` применяются следующие правила.
Каждое нарушение описывается объектом `CompatibilityIssue`:

```
// Проблема совместимости
message CompatibilityIssue {
  CompatibilityIssueType code = 1; // код нарушения совместимости
  string message = 2; // человекочитаемое описание
  string struct_name = 3; // имя структуры, в которой произошло нарушение
  int32 field_id = 4; // id поля, если применимо
}
```

### Разрешено ✅

| Действие | Примечание |
|---|---|
| Добавление нового **необязательного** поля (`required=false`) | Старые клиенты просто игнорируют его |
| Удаление **необязательного** поля | Старые клиенты получат нулевое значение |
| Добавление нового struct-а с любыми полями | Новый struct не ломает старые данные в существующих struct-ах |
| Изменение **порядка** полей | Идентификатором является `id`, не позиция |

### Запрещено ❌

| Нарушение | Код (`issue.code`) | Объяснение |
|---|---|---|
| Добавление **обязательного** поля в существующий struct | `ADDED_REQUIRED_FIELD` | Старые клиенты не заполнят его — сервер сломается |
| Удаление **обязательного** поля | `REMOVED_REQUIRED_FIELD` | Старые клиенты будут его отправлять — данные потеряются |
| Изменение **типа** поля (по `id`) | `FIELD_TYPE_CHANGED` | Несовместимые wire-форматы |
| Смена `required: false → true` | `OPTIONAL_TO_REQUIRED` | Старые клиенты не заполнят поле |
| Смена `id` существующего поля (по `name`) | `FIELD_ID_CHANGED` | Нарушает wire-формат |
| Дублирующиеся `id` внутри одного struct | `DUPLICATE_FIELD_ID` | Невалидная схема |
| Дублирующиеся `name` внутри одного struct | `DUPLICATE_FIELD_NAME` | Невалидная схема |
| Дублирующиеся имена struct внутри schema | `DUPLICATE_STRUCT_NAME` | Невалидная схема |

> Изменение `required: true → false` **разрешено** (делает поле менее строгим).

---

## Требования к реализации

1. **Язык:** Python 3.11 (версии ≥ 3.13 не поддерживаются текущими бинарными wheels gRPC).
2. **Точка входа:** `python src/server.py`
3. **Порт:** сервис должен слушать на `[::]:{PORT}`, где `PORT` берётся из переменной окружения (по умолчанию `50051`).
4. **Состояние:** хранение in-memory, персистентность не требуется.
5. **Контейнер:** образ должен собираться командой `docker build -t <tag> .` из корня репозитория.
6. **Генерация кода:** в `Dockerfile` уже настроен вызов `grpc_tools.protoc` — вы можете положиться на него или сгенерировать заранее.
7. **Не менять:** `proto/schema_registry.proto`, `tests/`, `requirements*.txt`.

### Структура `src/`

```
src/
    server.py          ← ваша реализация
    # сюда же попадут сгенерированные файлы при сборке Docker:
    schema_registry_pb2.py
    schema_registry_pb2_grpc.py
```

---

## Локальная разработка

### 1. Установить зависимости

```bash
python -m venv .venv
source .venv/bin/activate
./.venv/bin/pip install -r requirements.txt
```

### 2. Сгенерировать Python-код из `.proto`

```bash
./.venv/bin/python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./src \
    --grpc_python_out=./src \
    ./proto/schema_registry.proto
```

После этого в `src/` появятся `schema_registry_pb2.py` и `schema_registry_pb2_grpc.py`.

### 3. Запустить сервер локально

```bash
PYTHONPATH=src ./.venv/bin/python src/server.py
```

Вы можете протестировать свой сервер с помощью Postman (воспользуйтесь schema_registry.proto для этого).

### 4. Запустить тесты

Тесты **автоматически** собирают Docker-образ и поднимают контейнер, подключаясь к нему с хоста. Docker должен быть установлен и запущен.

```bash
./.venv/bin/python -m pytest tests/integration/ -q
```

---

## Как работают автотесты

Тесты написаны на pytest и живут в [tests/integration/test_container_integration.py](tests/integration/test_container_integration.py).

**Жизненный цикл сессии тестов:**
1. `generated_modules` (фикстура) — вызывает `grpc_tools.protoc` прямо на хосте, генерирует `pb2`-модули во временную директорию и импортирует их. Таким образом тесты не зависят от вашей сборки.
2. `running_container` (фикстура) — выполняет `docker build`, затем `docker run -d -p <случайный_порт>:50051`. Ждёт готовности gRPC-канала до 30 секунд. После завершения тестов останавливает контейнер.
3. Каждый тест открывает **свой** gRPC-канал к контейнеру и обращается к нему как к обычному удалённому сервису.

## Система оценивания (GitHub Classroom)

Каждый тест соответствует заявленному правилу совместимости. CI запускается автоматически при пуше в репозиторий.

```
pytest tests/integration/ -q
```

> GitHub Actions использует `docker` в `ubuntu-latest`-ранере, поэтому контейнер собирается и запускается в CI так же, как и локально.

---

## Пример взаимодействия (псевдокод)

```python
# v1: минимальная схема пользователя
v1 = Schema(structs=[
    Struct(name="User", fields=[
        Field(id=1, name="id",   type=FIELD_TYPE_I64,    required=True),
        Field(id=2, name="name", type=FIELD_TYPE_STRING, required=False),
    ])
])
res = stub.RegisterSchema(RegisterSchemaRequest(service_name="users", schema=v1))
# → accepted=True, version=1

# v2: добавляем необязательный email — OK
v2 = Schema(structs=[
    Struct(name="User", fields=[
        Field(id=1, name="id",    type=FIELD_TYPE_I64,    required=True),
        Field(id=2, name="name",  type=FIELD_TYPE_STRING, required=False),
        Field(id=3, name="email", type=FIELD_TYPE_STRING, required=False),
    ])
])
res = stub.RegisterSchema(RegisterSchemaRequest(service_name="users", schema=v2))
# → accepted=True, version=2

# v3: пытаемся добавить обязательный phone — ЗАПРЕЩЕНО
v3 = Schema(structs=[
    Struct(name="User", fields=[
        Field(id=1, name="id",    type=FIELD_TYPE_I64,    required=True),
        Field(id=2, name="name",  type=FIELD_TYPE_STRING, required=False),
        Field(id=3, name="email", type=FIELD_TYPE_STRING, required=False),
        Field(id=4, name="phone", type=FIELD_TYPE_STRING, required=True),  # ← проблема
    ])
])
res = stub.RegisterSchema(RegisterSchemaRequest(service_name="users", schema=v3))
# → accepted=False, version=2
# → issues=[CompatibilityIssue(code=ADDED_REQUIRED_FIELD, struct_name="User", field_id=4)]
```

---

## Частые ошибки

| Симптом | Вероятная причина |
|---|---|
| `ModuleNotFoundError: No module named 'grpc'` | Используется Python 3.13+; нужен Python ≤ 3.12 |
| `ModuleNotFoundError: No module named 'schema_registry_pb2'` | `PYTHONPATH` не включает `src/`; код не был сгенерирован |
| Тесты зависают при `running_container` | Docker не запущен или сервис не слушает порт `50051` |
| `StatusCode.UNIMPLEMENTED` от сервера | Метод не реализован — заглушка из базового класса |
| Тесты падают с `AssertionError` на `accepted` | Логика проверки совместимости работает неверно |

---

## Дополнительные материалы

- [gRPC Basics — Python](https://grpc.io/docs/languages/python/basics/)
- [Protocol Buffers Language Guide (proto3)](https://protobuf.dev/programming-guides/proto3/)
- [Apache Thrift IDL](https://thrift.apache.org/docs/idl)
- [Confluent Schema Registry — Compatibility Types](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html)
