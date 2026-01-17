# ScyllaDB WASM Proxy

Прокси-сервер для ScyllaDB, позволяющий загружать WASM-скрипты для маскирования конфиденциальных данных (PII) «на лету» перед тем, как они попадут в приложение.

## Особенности

- **Прозрачный TCP-прокси**: Перехватывает трафик между клиентом и ScyllaDB.
- **WASM-трансформации**: Загружает пользовательские WebAssembly-скрипты для модификации запросов/ответов.
- **Маскирование PII**: Пример использования — скрытие email, телефонов, SSN в реальном времени.
- **Гибкая конфигурация**: Настройка адресов, каталога скриптов, уровня логирования.
- **Поддержка TLS**: Опциональное шифрование трафика между клиентом и прокси.
- **Написано на Go**: Высокая производительность, низкие задержки.

## Архитектура

```
Клиент → Прокси (порт 9042) → Трансформация запроса (WASM) → ScyllaDB (порт 9043)
        ↑                                                                  ↓
        ← Трансформация ответа (WASM) ← Ответ ←
```

## Установка

1. Клонируйте репозиторий:
```bash
git clone <repo>
cd scylla-proxy
```

2. Установите зависимости:
```bash
go mod tidy
```

3. Соберите:
```bash
go build -o scylla-proxy ./cmd
```

## Конфигурация

Создайте `config.yaml`:
```yaml
listen_addr: "127.0.0.1:9042"
target_addr: "127.0.0.1:9043"
wasm_dir: "./wasm_scripts"
log_level: "info"
tls:
  enabled: false
  cert_file: "certs/server.crt"
  key_file: "certs/server.key"
```

## WASM-скрипты

Поместите `.wasm` файлы в каталог `wasm_scripts/`. Каждый модуль должен экспортировать функцию `transform` с сигнатурой:

```wasm
(func (export "transform") (param i32 i32) (result i32))
```
- Параметры: указатель на входные данные и длина.
- Возвращает указатель на выходные данные (длина записывается в первых 4 байтах).

Пример скрипта см. в `wasm_scripts/example.wat`. Скомпилируйте его:
```bash
wat2wasm example.wat -o example.wasm
```

## Запуск

```bash
./scylla-proxy -config config.yaml
```

Прокси будет слушать на `listen_addr` и пересылать трафик на `target_addr`.

## Тестирование

### Интеграционные тесты:
```bash
./scripts/test_integration.sh
```

### Тестовый клиент:
```bash
cd test
go run test_proxy.go
```

## Структура проекта

```
scylla-proxy/
├── cmd/main.go              # Точка входа
├── certs/                   # Сертификаты TLS
│   ├── server.crt
│   └── server.key
├── internal/
│   ├── config/              # Загрузка конфигурации
│   ├── proxy/               # Логика TCP-прокси
│   │   ├── proxy.go
│   │   └── transform.go
│   └── wasm/                # Движок WASM (wazero)
│       └── engine.go
├── scripts/                 # Скрипты для разработки и тестирования
│   ├── echo_server.go
│   ├── start_scylla_mock.sh
│   └── test_integration.sh
├── test/                    # Тесты
│   ├── test_proxy.go
│   └── wasm_scripts/
├── wasm_scripts/            # Пользовательские WASM-скрипты
│   ├── example.wat
│   └── example.wasm
├── config.yaml              # Конфигурация по умолчанию
├── go.mod
└── README.md
```

## Зависимости

- Go 1.23+
- [wazero](https://github.com/tetratelabs/wazero) — среда выполнения WebAssembly
- [yaml.v2](https://gopkg.in/yaml.v2) — парсинг YAML

## Разработка

1. Реализуйте нужную логику маскирования в WASM (например, на Rust/AssemblyScript).
2. Скомпилируйте в `.wasm`.
3. Положите в `wasm_scripts/`.
4. Перезапустите прокси.

## Лицензия

MIT