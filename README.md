***Утилита загрузки файлов с информацией об организациях***
Данные находятся в файлах данных фомата COBOL. Бинарные файлы с символами где поля имеют ограниченную длину и
следуют друг за другом.
Описание структуры в формате COBOL PIC (COPYBOOK)

Задача: Взять файл, анмаршалить его по описанию структуры и записать в SQL базу данных.Abstract

Имеются следующие виды описаний:
ANNUAL_MICRO_DATA_FILE(содержат ANNUAL_MICRO_DATA_REC)

Необходимо разработать следующий алгоритм:
чтение файла
преобразование в структуру
запись в SQL

Имеется готовое решение по генерации структуры по описанию github.com/foundatn-io/go-pic
модуль немного не так работает как должен но сгодится

ТODO: Определиться со структурой (подумать нужно ли отделить информацию о принципалах(ANNUAL_PRINCIPALS)


delete from annual_microdata where 1=1;

ALTER TABLE annual_microdata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci

ALTER TABLE dbname.annual_microdata CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER DATABASE dbname CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

c:\wamp\bin\mysql\mysql5.5.20\bin>mysql -u root dbname < c:\wamp\bin\mysql\mysql5.5.20\bin\annual_microdata_all.sql
c:\wamp\bin\mysql\mysql5.5.20\bin>mysqldump -u root --no-data --set-charset --compress dbname annual_microdata > dump-defs.sql

psql --port "5433" -d dbname -f dump_1252-2