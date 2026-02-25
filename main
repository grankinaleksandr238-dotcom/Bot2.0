#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ОДНОРАЗОВЫЙ СКРИПТ ДЛЯ ИМПОРТА ПОЛЬЗОВАТЕЛЕЙ ИЗ CSV В БАЗУ.
Загружает ВСЕ поля: user_id, username, first_name, balance, reputation и т.д.
Запуск: python import_users.py
"""

import asyncio
import csv
import os
from datetime import datetime

import asyncpg

# ========== НАСТРОЙКИ ==========
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/dbname")
CSV_FILE = "users.csv"  # имя вашего файла (положите рядом со скриптом)

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========
async def main():
    print("🔌 Подключаюсь к базе...")
    conn = await asyncpg.connect(DATABASE_URL)

    print(f"📂 Читаю файл {CSV_FILE}...")
    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            print("❌ Ошибка: файл пуст или нет заголовков")
            await conn.close()
            return

        print(f"📋 Найденные колонки: {', '.join(reader.fieldnames)}")

        # Разрешённые поля (ВСЕ, что есть в таблице users)
        allowed_columns = {
            'user_id', 'username', 'first_name', 'joined_date', 'balance', 'reputation',
            'total_spent', 'negative_balance', 'last_bonus', 'last_theft_time',
            'theft_attempts', 'theft_success', 'theft_failed', 'theft_protected',
            'casino_wins', 'casino_losses', 'dice_wins', 'dice_losses', 'guess_wins',
            'guess_losses', 'slots_wins', 'slots_losses', 'roulette_wins', 'roulette_losses',
            'exp', 'level', 'last_gift_time', 'gift_count_today', 'global_authority',
            'smuggle_success', 'smuggle_fail', 'bitcoin_balance', 'authority_balance',
            'skill_share', 'skill_luck', 'skill_betray', 'heists_joined',
            'heists_betray_attempts', 'heists_betray_success', 'heists_betrayed_count',
            'heists_earned', 'strength', 'agility', 'defense'
        }

        # Соответствие полей (если в CSV названия отличаются от БД)
        field_mapping = {
            'global_authority': 'authority_balance',  # если в CSV есть такое поле
            # добавьте другие, если нужно
        }

        inserted = 0
        updated = 0
        errors = []

        for row_num, row in enumerate(reader, start=2):
            try:
                # Применяем маппинг полей и фильтруем только разрешённые
                mapped = {}
                for csv_field, value in row.items():
                    if value == '':
                        continue
                    db_field = field_mapping.get(csv_field, csv_field)
                    if db_field in allowed_columns:
                        mapped[db_field] = value

                # Проверяем обязательное поле
                if 'user_id' not in mapped:
                    errors.append(f"Строка {row_num}: пропущена (нет user_id)")
                    continue

                user_id = int(mapped['user_id'])

                # === ПРЕОБРАЗОВАНИЕ ТИПОВ ===

                # Числа с плавающей точкой (балансы и т.д.)
                for key in ['balance', 'total_spent', 'negative_balance', 'bitcoin_balance', 'heists_earned']:
                    if key in mapped:
                        try:
                            mapped[key] = float(mapped[key]) if mapped[key] else 0.0
                        except:
                            mapped[key] = 0.0

                # Целые числа (репутация, уровни, счётчики)
                int_keys = [
                    'reputation', 'theft_attempts', 'theft_success', 'theft_failed', 'theft_protected',
                    'casino_wins', 'casino_losses', 'dice_wins', 'dice_losses', 'guess_wins', 'guess_losses',
                    'slots_wins', 'slots_losses', 'roulette_wins', 'roulette_losses', 'exp', 'level',
                    'gift_count_today', 'global_authority', 'smuggle_success', 'smuggle_fail',
                    'authority_balance', 'skill_share', 'skill_luck', 'skill_betray',
                    'heists_joined', 'heists_betray_attempts', 'heists_betray_success',
                    'heists_betrayed_count', 'strength', 'agility', 'defense'
                ]
                for key in int_keys:
                    if key in mapped:
                        try:
                            mapped[key] = int(float(mapped[key]))
                        except:
                            mapped[key] = 0

                # Даты
                date_keys = ['joined_date', 'last_bonus', 'last_theft_time', 'last_gift_time']
                for key in date_keys:
                    if key in mapped and mapped[key]:
                        try:
                            mapped[key] = datetime.strptime(mapped[key], "%Y-%m-%d %H:%M:%S")
                        except:
                            mapped[key] = None
                    else:
                        mapped[key] = None

                # === ВСТАВКА ИЛИ ОБНОВЛЕНИЕ (UPSERT) ===

                columns = ', '.join(mapped.keys())
                placeholders = ', '.join([f'${i+1}' for i in range(len(mapped))])
                values = list(mapped.values())

                # Проверяем, существует ли уже пользователь
                exists = await conn.fetchval("SELECT 1 FROM users WHERE user_id = $1", user_id)

                if exists:
                    # Обновляем
                    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in mapped.keys() if col != 'user_id'])
                    query = f"""
                        INSERT INTO users ({columns})
                        VALUES ({placeholders})
                        ON CONFLICT (user_id) DO UPDATE SET {update_set}
                    """
                    await conn.execute(query, *values)
                    updated += 1
                else:
                    # Вставляем нового
                    query = f"INSERT INTO users ({columns}) VALUES ({placeholders})"
                    await conn.execute(query, *values)
                    inserted += 1

            except Exception as e:
                errors.append(f"Строка {row_num} (ID {row.get('user_id')}): {str(e)}")
                continue

    # === ИТОГ ===
    print("\n" + "="*50)
    print(f"✅ ИМПОРТ ЗАВЕРШЁН!")
    print(f"📥 Добавлено новых пользователей: {inserted}")
    print(f"🔄 Обновлено существующих: {updated}")
    if errors:
        print(f"❌ Ошибок: {len(errors)}")
        for err in errors[:5]:  # покажем первые 5 ошибок
            print(f"  • {err}")
    else:
        print("✅ Ошибок нет")
    print("="*50)

    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
