#!/usr/bin/env python3
"""
Универсальная миграция MySQL → ClickHouse.

- Выводит список таблиц из MySQL и спрашивает, какие копировать.
- Автоматически определяет типы столбцов и создаёт соответствующие типы в ClickHouse.
- Копирует данные потоково (fetchmany), с контролем памяти CH.
- После копирования сверяет полноту данных (количество строк) и предлагает перезагрузить
  таблицы, которые отсутствуют в CH или отличаются от источника.

Переменные окружения — в .env (см. .env.example).

Запуск:
  pip install -r requirements-migrate.txt
  cd server
  python migrate_mysql_to_ch_universal.py
"""

import os
import re
import sys
import json
import time
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

import mysql.connector

# --- Конфиг из .env ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "retail")

CH_HOST = os.environ.get("CH_HOST", "localhost")
CH_PORT = int(os.environ.get("CH_PORT", "8123"))
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DATABASE = os.environ.get("CH_DATABASE", "default")

BATCH_SIZE_DEFAULT = int(os.environ.get("MIGRATE_BATCH_SIZE", "2000"))
BATCH_SIZE_LARGE = int(os.environ.get("MIGRATE_BATCH_SIZE_LARGE", "500"))
MEMORY_HIGH_GB = float(os.environ.get("CH_MEMORY_HIGH_GB", "4.5"))
MEMORY_TARGET_GB = float(os.environ.get("CH_MEMORY_TARGET_GB", "3.5"))
MEMORY_POLL_MS = int(os.environ.get("CH_MEMORY_POLL_MS", "2000"))
MEMORY_MAX_WAIT_MS = int(os.environ.get("CH_MEMORY_MAX_WAIT_MS", "600000"))


def mysql_type_to_clickhouse(data_type: str, column_type: str, is_nullable: str,
                             numeric_precision: Optional[int], numeric_scale: Optional[int],
                             char_max_length: Optional[int]) -> str:
    """Преобразует тип MySQL в тип ClickHouse."""
    data_type = (data_type or "").lower().strip()
    column_type = (column_type or "").lower()
    nullable = (is_nullable or "").upper() == "YES"

    def wrap(t: str) -> str:
        return f"Nullable({t})" if nullable else t

    # Целые
    if data_type in ("int", "integer", "mediumint"):
        if "unsigned" in column_type:
            return wrap("UInt32")
        return wrap("Int32")
    if data_type == "bigint":
        if "unsigned" in column_type:
            return wrap("UInt64")
        return wrap("Int64")
    if data_type == "smallint":
        if "unsigned" in column_type:
            return wrap("UInt16")
        return wrap("Int16")
    if data_type == "tinyint":
        if "unsigned" in column_type:
            return wrap("UInt8")
        return wrap("Int8")

    # Дробные
    if data_type == "decimal":
        p = numeric_precision or 10
        s = numeric_scale if numeric_scale is not None else 2
        p = min(p, 65)
        return wrap(f"Decimal({p}, {s})")
    if data_type == "float":
        return wrap("Float32")
    if data_type in ("double", "real"):
        return wrap("Float64")

    # Дата/время
    if data_type == "date":
        return wrap("Date")
    if data_type in ("datetime", "timestamp"):
        return wrap("DateTime")

    # Строки и бинарные
    if data_type in ("varchar", "char", "text", "tinytext", "mediumtext", "longtext",
                    "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob",
                    "json", "set", "enum"):
        return wrap("String")

    if data_type == "bit":
        return wrap("UInt8")
    if data_type == "year":
        return wrap("UInt16")

    # По умолчанию
    return wrap("String")


def quote_ident(name: str) -> str:
    """Экранирует идентификатор для ClickHouse (обратные кавычки)."""
    return "`" + str(name).replace("`", "\\`") + "`"


def get_mysql_tables(conn) -> list:
    """Возвращает список таблиц в базе MySQL."""
    cur = conn.cursor()
    cur.execute(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
        (DB_NAME,),
    )
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    return tables


def get_mysql_columns(conn, table: str) -> list:
    """Возвращает описание столбцов таблицы из information_schema."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE,
               NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (DB_NAME, table),
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "name": r[0],
            "data_type": r[1],
            "column_type": r[2],
            "is_nullable": r[3],
            "numeric_precision": r[4],
            "numeric_scale": r[5],
            "char_max_length": r[6],
        }
        for r in rows
    ]


def build_clickhouse_ddl(table: str, columns: list) -> str:
    """Строит CREATE TABLE для ClickHouse по списку столбцов MySQL."""
    parts = []
    order_cols = []
    has_nullable_key = False
    for col in columns:
        ch_type = mysql_type_to_clickhouse(
            col["data_type"],
            col["column_type"] or "",
            col["is_nullable"] or "NO",
            col["numeric_precision"],
            col["numeric_scale"],
            col["char_max_length"],
        )
        if "Nullable(" in ch_type:
            has_nullable_key = True
        name = quote_ident(col["name"])
        parts.append(f"  {name} {ch_type}")
        if len(order_cols) < 3:
            order_cols.append(name)
    cols_ddl = ",\n".join(parts)
    order_clause = ", ".join(order_cols)
    settings = ""
    if has_nullable_key:
        settings = " SETTINGS index_granularity = 8192, allow_nullable_key = 1"
    else:
        settings = " SETTINGS index_granularity = 8192"
    return (
        f"CREATE TABLE IF NOT EXISTS {CH_DATABASE}.{quote_ident(table)} (\n"
        f"{cols_ddl}\n"
        f") ENGINE = MergeTree()\n"
        f"ORDER BY ({order_clause}){settings}"
    )


def ch_query(sql: str, body: Optional[bytes] = None, timeout: int = 300) -> tuple:
    """Выполняет запрос к ClickHouse по HTTP."""
    url = f"http://{CH_HOST}:{CH_PORT}/?database={CH_DATABASE}"
    data = body if body is not None else sql.encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            "X-ClickHouse-User": CH_USER,
            "X-ClickHouse-Key": CH_PASSWORD or "",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8"), r.status
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"ClickHouse HTTP {e.code}: {e.read().decode('utf-8', errors='replace')}")


def ch_query_json(sql: str) -> list:
    """Выполняет SELECT и возвращает список словарей."""
    resp, _ = ch_query(sql + " FORMAT JSONEachRow")
    out = []
    for line in resp.strip().split("\n"):
        if line:
            out.append(json.loads(line))
    return out


def ch_memory_bytes() -> Optional[int]:
    try:
        rows = ch_query_json("SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'")
        if rows:
            return int(rows[0].get("value", 0))
    except Exception:
        pass
    return None


def wait_for_memory():
    target = MEMORY_TARGET_GB * 1e9
    start = time.monotonic()
    last_log = 0
    while (time.monotonic() - start) * 1000 < MEMORY_MAX_WAIT_MS:
        mem = ch_memory_bytes()
        if mem is None or mem < target:
            return
        if time.monotonic() - last_log >= 5:
            print(f"  [память CH] {mem/1e9:.2f} GiB — ждём до {MEMORY_TARGET_GB} GiB...")
            last_log = time.monotonic()
        time.sleep(MEMORY_POLL_MS / 1000)
    raise RuntimeError(
        f"Память CH не опустилась ниже {MEMORY_TARGET_GB} GiB за {MEMORY_MAX_WAIT_MS/1000} с."
    )


def value_to_ch(value, col_info: dict) -> Optional[str | int | float]:
    """Приводит значение из MySQL к виду для JSON (ClickHouse JSONEachRow)."""
    if value is None:
        return None
    dt = (col_info.get("data_type") or "").lower()
    if dt in ("date", "datetime", "timestamp"):
        if hasattr(value, "strftime"):
            if dt == "date":
                return value.strftime("%Y-%m-%d")
            return value.strftime("%Y-%m-%d %H:%M:%S")
        s = str(value)
        return s[:10] if dt == "date" else (s[:19] if len(s) >= 19 else s)
    if dt in ("int", "integer", "bigint", "smallint", "tinyint", "mediumint"):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    if dt in ("decimal", "float", "double", "real"):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return str(value)


def migrate_one_table(mysql_conn, table: str, columns: list, batch_size: int) -> int:
    """Копирует одну таблицу из MySQL в ClickHouse. Возвращает число скопированных строк."""
    col_names = [c["name"] for c in columns]
    cols_sql = ", ".join(quote_ident(c) for c in col_names)
    order_sql = ", ".join(quote_ident(c) for c in col_names[:3]) if len(col_names) >= 3 else quote_ident(col_names[0])
    select_sql = f"SELECT {cols_sql} FROM {quote_ident(table)} ORDER BY {order_sql}"

    cur = mysql_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}")
    total = cur.fetchone()[0]
    cur.close()

    print(f"  Строк в MySQL: {total}")
    if total == 0:
        print(f"  ✓ {table}: 0 строк (пустая)")
        return 0

    cursor = mysql_conn.cursor()
    cursor.execute(select_sql)
    col_names_order = [d[0] for d in cursor.description]
    migrated = 0
    log_every = max(batch_size * 50, 25000)
    last_log = 0

    try:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            batch = []
            for row in rows:
                d = dict(zip(col_names_order, row))
                out = {}
                for c in columns:
                    v = value_to_ch(d.get(c["name"]), c)
                    out[c["name"]] = v
                batch.append(out)

            mem = ch_memory_bytes()
            if mem is not None and mem >= MEMORY_HIGH_GB * 1e9:
                print(f"  Перенесено: {migrated}/{total} — память CH {mem/1e9:.2f} GiB, ждём...")
                wait_for_memory()

            lines = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in batch)
            body = f"INSERT INTO {CH_DATABASE}.{quote_ident(table)} FORMAT JSONEachRow\n{lines}".encode("utf-8")
            ch_query("", body=body)
            migrated += len(batch)
            del batch
            if migrated - last_log >= log_every or migrated <= batch_size:
                print(f"  Перенесено: {migrated}/{total} ({100*migrated/total:.1f}%)")
                last_log = migrated
        print(f"  ✓ {table}: {migrated} строк")
    finally:
        cursor.close()
    return migrated


def count_mysql(conn, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}")
    n = cur.fetchone()[0]
    cur.close()
    return n


def count_clickhouse(table: str) -> Optional[int]:
    try:
        rows = ch_query_json(f"SELECT count() AS c FROM {CH_DATABASE}.{quote_ident(table)}")
        return int(rows[0]["c"]) if rows else None
    except Exception:
        return None


def main():
    print("=== Универсальная миграция MySQL → ClickHouse ===\n")
    print(f"MySQL: {DB_HOST}:{DB_PORT} {DB_NAME}")
    print(f"ClickHouse: {CH_HOST}:{CH_PORT} {CH_DATABASE}\n")

    try:
        mysql_conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            use_pure=True,
        )
    except mysql.connector.Error as e:
        print(f"Ошибка подключения к MySQL: {e}")
        sys.exit(1)

    try:
        tables = get_mysql_tables(mysql_conn)
    except Exception as e:
        print(f"Ошибка чтения списка таблиц MySQL: {e}")
        mysql_conn.close()
        sys.exit(1)

    if not tables:
        print("В базе MySQL нет таблиц.")
        mysql_conn.close()
        return

    print(f"Найдено таблиц в MySQL: {len(tables)}\n")
    for i, t in enumerate(tables, 1):
        print(f"  {i}. {t}")
    print("\nВведите номера таблиц через пробел (например: 1 3 5) или 'all' для всех:")
    choice = input("> ").strip().lower()
    if not choice:
        print("Ничего не выбрано. Выход.")
        mysql_conn.close()
        return

    if choice == "all":
        selected = list(tables)
    else:
        try:
            indices = [int(x) for x in choice.split()]
            selected = [tables[i - 1] for i in indices if 1 <= i <= len(tables)]
        except ValueError:
            print("Неверный ввод. Выход.")
            mysql_conn.close()
            return
    if not selected:
        print("Нет выбранных таблиц.")
        mysql_conn.close()
        return

    print(f"\nВыбрано таблиц: {len(selected)}: {', '.join(selected)}\n")

    start_time = time.time()
    migrated_tables = []

    for table in selected:
        print(f"\n=== Таблица: {table} ===")
        try:
            columns = get_mysql_columns(mysql_conn, table)
        except Exception as e:
            print(f"  Ошибка чтения структуры: {e}")
            continue
        if not columns:
            print("  Нет столбцов — пропуск.")
            continue

        ddl = build_clickhouse_ddl(table, columns)
        try:
            ch_query(f"DROP TABLE IF EXISTS {CH_DATABASE}.{quote_ident(table)}")
            ch_query(ddl)
            print("  Создана таблица в ClickHouse.")
        except Exception as e:
            print(f"  Ошибка создания таблицы в CH: {e}")
            continue

        total_rows = count_mysql(mysql_conn, table)
        batch_size = BATCH_SIZE_LARGE if total_rows > 1_000_000 else BATCH_SIZE_DEFAULT
        try:
            n = migrate_one_table(mysql_conn, table, columns, batch_size)
            migrated_tables.append((table, n))
        except Exception as e:
            print(f"  Ошибка миграции: {e}")
            import traceback
            traceback.print_exc()

    mysql_conn.close()

    # --- Сверка полноты данных ---
    print("\n=== Сверка полноты данных ===")
    to_reload = []
    try:
        verify_conn = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, database=DB_NAME, use_pure=True,
        )
    except Exception:
        verify_conn = None
    for table in selected:
        mysql_count = None
        if verify_conn:
            try:
                mysql_count = count_mysql(verify_conn, table)
            except Exception:
                pass
        ch_count = count_clickhouse(table)
        if ch_count is None:
            print(f"  {table}: в ClickHouse таблица отсутствует или недоступна")
            to_reload.append(table)
        elif mysql_count is not None and mysql_count != ch_count:
            print(f"  {table}: MySQL={mysql_count}, ClickHouse={ch_count} — не совпадает")
            to_reload.append(table)
        else:
            print(f"  {table}: OK (строк: {ch_count})")

    if to_reload:
        print(f"\nТаблицы для перезагрузки (отсутствуют или неполные): {', '.join(to_reload)}")
        print("Перезагрузить их сейчас? (y/n):")
        if input("> ").strip().lower() == "y":
            mysql_conn = mysql.connector.connect(
                host=DB_HOST, port=DB_PORT, user=DB_USER,
                password=DB_PASSWORD, database=DB_NAME, use_pure=True,
            )
            for table in to_reload:
                try:
                    columns = get_mysql_columns(mysql_conn, table)
                    if not columns:
                        continue
                    ch_query(f"TRUNCATE TABLE IF EXISTS {CH_DATABASE}.{quote_ident(table)}")
                    total_rows = count_mysql(mysql_conn, table)
                    batch_size = BATCH_SIZE_LARGE if total_rows > 1_000_000 else BATCH_SIZE_DEFAULT
                    print(f"\n=== Перезагрузка: {table} ===")
                    migrate_one_table(mysql_conn, table, columns, batch_size)
                except Exception as e:
                    print(f"  Ошибка перезагрузки {table}: {e}")
            mysql_conn.close()
            print("\nПерезагрузка завершена.")
    else:
        print("\nВсе выбранные таблицы присутствуют и полны.")
    if verify_conn:
        try:
            verify_conn.close()
        except Exception:
            pass

    elapsed = time.time() - start_time
    print(f"\n=== Миграция завершена за {elapsed/60:.1f} мин ===")


if __name__ == "__main__":
    main()
