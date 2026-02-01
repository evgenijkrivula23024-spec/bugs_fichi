migrate_mysql_to_ch_universal.py c переменными окружения. просто создай .env и заполни
# Пример конфигурации для миграции MySQL → ClickHouse
# Скопируйте в .env и заполните своими значениями.

# === MySQL (источник) ===
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=
DB_NAME=retail

# === ClickHouse (приёмник) ===
CH_HOST=localhost
CH_PORT=8123
CH_USER=default
CH_PASSWORD=
CH_DATABASE=default
