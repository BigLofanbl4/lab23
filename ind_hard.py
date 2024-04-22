# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import typing as t

import psycopg2


DATABASE = "ama"
USER = "postgres"
PASSWORD = "123"
HOST = "localhost"
PORT = "5432"


def create_db() -> None:
    """
    Создать базу данных.
    """
    conn = psycopg2.connect(
        database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT
    )
    cursor = conn.cursor()

    # Создать таблицу с информацией о зодиаках.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS zodiacs (
            zodiac_id SERIAL PRIMARY KEY,
            zodiac_title TEXT NOT NULL
        )
        """
    )

    # Создание таблицы людей
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS people (
            person_id SERIAL PRIMARY KEY,
            surname TEXT NOT NULL,
            name TEXT NOT NULL,
            zodiac_id INTEGER NOT NULL REFERENCES zodiacs(zodiac_id),
            birthday TEXT NOT NULL
        )
    """
    )

    conn.commit()
    conn.close()


def add_person(surname: str, name: str, zodiac: str, birthday: str) -> None:
    """
    Добавляет работника в базу данных.
    """
    conn = psycopg2.connect(
        database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT
    )
    cursor = conn.cursor()

    # Получить идентификатор зодиака в базе данных.
    # Если такой записи нет, то добавить информацию о новом зодике.
    cursor.execute(
        """
        SELECT zodiac_id FROM zodiacs WHERE zodiac_title = %s
        """,
        (zodiac,),
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            "INSERT INTO zodiacs (zodiac_title) VALUES (%s) RETURNING zodiac_id",
            (zodiac,),
        )
        zodiac_id = cursor.fetchone()[0]
    else:
        zodiac_id = row[0]

    # Добавить информацию о новом человеке.
    cursor.execute(
        """
        INSERT INTO people (surname, name, zodiac_id, birthday)
        VALUES (%s, %s, %s, %s)
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


def select_all() -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать всех людей.
    """
    conn = psycopg2.connect(
        database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT people.surname, people.name, zodiacs.zodiac_title, people.birthday
        FROM people
        INNER JOIN zodiacs ON zodiacs.zodiac_id = people.zodiac_id
        """
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


def select_by_surname(surname: str) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать людей с заданной фамилией.
    """
    conn = psycopg2.connect(
        database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT
    )
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT people.surname, people.name, zodiacs.zodiac_title, people.birthday
        FROM people
        INNER JOIN zodiacs ON zodiacs.zodiac_id = people.zodiac_id
        WHERE people.surname = %s
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
    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("people")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")

    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления человека.
    add = subparsers.add_parser("add", help="Add a new person")
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
        type=str,
        required=True,
        help="The person's birthday",
    )

    # Создать субпарсер для отображения всех людей.
    _ = subparsers.add_parser("display", help="Display people")

    # Создать субпарсер для выбора людей по фамилии.
    select = subparsers.add_parser("select", help="Select people by surname")
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
    create_db()

    match args.command:
        case "add":
            add_person(args.surname, args.name, args.zodiac, args.birthday)

        case "select":
            display_people(select_by_surname(args.surname))

        case "display":
            display_people(select_all())


if __name__ == "__main__":
    main()
