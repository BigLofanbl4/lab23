# !/usr/bin/env python3
# -*- coding: utf-8 -*-

#Для своего варианта лабораторной работы 2.17 необходимо реализовать хранение данных в
#базе данных SQLite3. Информация в базе данных должна храниться не менее чем в двух
#таблицах.

import argparse
import sqlite3
import typing as t
from datetime import datetime
from pathlib import Path


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Создать таблицу с информацией о зодиаках.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS zodiacs (
            zodiac_id INTEGER PRIMARY KEY AUTOINCREMENT,
            zodiac_title TEXT NOT NULL
        )
        """
    )

    # Создание таблицы людей
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS people (
            person_id INTEGER PRIMARY KEY AUTOINCREMENT,
            surname TEXT NOT NULL,
            name TEXT NOT NULL,
            zodiac_id INTEGER NOT NULL,
            birthday Date NOT NULL,
            FOREIGN KEY(zodiac_id) REFERENCES zodiacs(zodiac_id)
        )
    """
    )

    conn.commit()
    conn.close()


def add_person(
    database_path: Path,
    surname: str,
    name: str,
    zodiac: str,
    birthday: datetime,
) -> None:
    """
    Добавляет работника в базу данных.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Получить идентификатор зодиака в базе данных.
    # Если такой записи нет, то добавить информацию о новом зодике.
    cursor.execute(
        """
        SELECT zodiac_id FROM zodiacs WHERE zodiac_title = ?
        """,
        (zodiac,),
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO zodiacs (zodiac_title) VALUES (?)
            """,
            (zodiac,),
        )
        zodiac_id = cursor.lastrowid
    else:
        zodiac_id = row[0]

    # Добавить информацию о новом человеке.
    cursor.execute(
        """
        INSERT INTO people (surname, name, zodiac_id, birthday)
        VALUES (?, ?, ?, ?)
        """,
        (surname, name, zodiac_id, birthday),
    )

    conn.commit()
    conn.close()


def display_people(people: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить список людей.
    """
    if people:
        line = "+-{}-+-{}-+-{}-+-{}-+-{}-+".format(
            "-" * 4, "-" * 30, "-" * 30, "-" * 20, "-" * 20
        )
        print(line)
        print(
            "| {:^4} | {:^30} | {:^30} | {:^20} | {:^20} |".format(
                "№", "Фамилия", "Имя", "Знак зодиака", "Дата рождения"
            )
        )
        print(line)

        for idx, person in enumerate(people, 1):
            print(
                "| {:>4} | {:<30} | {:<30} | {:<20} | {:>20} |".format(
                    idx,
                    person.get("surname", ""),
                    person.get("name", ""),
                    person.get("zodiac", ""),
                    person.get("birthday", ""),
                )
            )
        print(line)
    else:
        print("Список пуст")


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех людей.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT people.surname, people.name, zodiacs.zodiac_title, people.birthday
        FROM people
        INNER JOIN zodiacs ON zodiacs.zodiac_id = people.zodiac_id
        """
    )
    rows = cursor.fetchall()

    conn.close
    return [
        {
            "surname": row[0],
            "name": row[1],
            "zodiac": row[2],
            "birthday": row[3],
        }
        for row in rows
    ]


def select_by_surname(
    database_path: Path, surname: str
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать людей с заданной фамилией.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT people.surname, people.name, zodiacs.zodiac_title, people.birthday
        FROM people
        INNER JOIN zodiacs ON zodiacs.zodiac_id = people.zodiac_id
        WHERE people.surname = ?
        """,
        (surname,),
    )
    rows = cursor.fetchall()

    conn.close()

    return [
        {
            "surname": row[0],
            "name": row[1],
            "zodiac": row[2],
            "birthday": row[3],
        }
        for row in rows
    ]


def main(command_line=None):
    """
    Главная функция программы.
    """
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        type=str,
        required=False,
        default=str(Path.home() / "workers.db"),
        help="The database file name",
    )

    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("people")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")

    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления человека.
    add = subparsers.add_parser(
        "add", parents=[file_parser], help="Add a new person"
    )
    add.add_argument(
        "-s",
        "--surname",
        action="store",
        required=True,
        help="The person's surname",
    )
    add.add_argument(
        "-n", "--name", action="store", required=True, help="The person's name"
    )
    add.add_argument(
        "-z", "--zodiac", action="store", help="The person's zodiac"
    )
    add.add_argument(
        "-b",
        "--birthday",
        action="store",
        required=True,
        help="The person's birthday",
    )

    # Создать субпарсер для отображения всех людей.
    _ = subparsers.add_parser(
        "display", parents=[file_parser], help="Display people"
    )

    # Создать субпарсер для выбора людей по фамилии.
    select = subparsers.add_parser(
        "select", parents=[file_parser], help="Select people by surname"
    )
    select.add_argument(
        "-s",
        "--surname",
        action="store",
        type=str,
        required=True,
        help="The required surname",
    )

    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)

    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)

    match args.command:
        case "add":
            add_person(
                db_path, args.surname, args.name, args.zodiac, args.birthday
            )

        case "select":
            display_people(select_by_surname(db_path, args.surname))

        case "display":
            display_people(select_all(db_path))


if __name__ == "__main__":
    main()
