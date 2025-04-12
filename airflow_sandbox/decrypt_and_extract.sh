#!/bin/bash

# Перевірка параметрів
if [ $# -ne 2 ]; then
  echo "Використання: $0 <фінальний_архів> <приватний_ключ>"
  exit 1
fi

ENCRYPTED_ARCHIVE=$1
PRIVATE_KEY=$2

# Папки для роботи
TEMP_DIR="temp_parts"
DECRYPTED_DIR="decrypted_parts"
RECONSTRUCTED_ARCHIVE="reconstructed_archive.tar.gz"

mkdir -p "$TEMP_DIR" "$DECRYPTED_DIR"

# Крок 1: Розпаковуємо зашифрований архів
echo "Розпаковуємо архів $ENCRYPTED_ARCHIVE..."
tar -xzf "$ENCRYPTED_ARCHIVE" -C "$TEMP_DIR" || { echo "Помилка при розпаковуванні архіву!"; exit 1; }

# Крок 2: Дешифруємо кожну частину
echo "Дешифруємо частини..."
for enc_part in "$TEMP_DIR"/part_*.enc; do
  OUTPUT_PART="${DECRYPTED_DIR}/$(basename "$enc_part" .enc)"
  openssl rsautl -decrypt -inkey "$PRIVATE_KEY" -in "$enc_part" -out "$OUTPUT_PART" || { echo "Помилка при дешифруванні $enc_part"; exit 1; }
  echo "Дешифровано: $OUTPUT_PART"
done

# Крок 3: Об'єднуємо частини у вихідний архів
echo "Об'єднуємо частини у архів $RECONSTRUCTED_ARCHIVE..."
cat "$DECRYPTED_DIR"/part_* > "$RECONSTRUCTED_ARCHIVE" || { echo "Помилка при об'єднанні частин!"; exit 1; }

# Перевірка створеного архіву
echo "Перевіряємо архів $RECONSTRUCTED_ARCHIVE..."
tar -tzf "$RECONSTRUCTED_ARCHIVE" || { echo "Архів пошкоджений!"; exit 1; }

# Крок 4: Розпаковуємо архів
echo "Розпаковуємо архів для відновлення файлів..."
tar -xzf "$RECONSTRUCTED_ARCHIVE" || { echo "Помилка при розпаковуванні архіву!"; exit 1; }

# Очищаємо тимчасові файли
rm -rf "$TEMP_DIR" "$DECRYPTED_DIR" "$RECONSTRUCTED_ARCHIVE"

echo "Файл успішно відновлено!"