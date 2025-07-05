import simplejson
import sqlite3
import psycopg
import csv

# -- Konfiguracja --
DB_TYPE = 'postgres'  
DB_PATH = 'baza_danych.db'
BACKUP_PATH = 'kopie/baza_backup.db'
TABLE_NAME_1 = 'użytkownicy'
TABLE_NAME_2 = 'książki'


# -- Połączenie z bazą danych --
def connect_db():
    if DB_TYPE == 'sqlite':
        return sqlite3.connect("/home/student04/Lab8)")
    elif DB_TYPE == 'postgres':
        with open("/home/student04/Lab8/database_creds.json") as db_con_file:
            creds = simplejson.loads(db_con_file.read())
            connection = psycopg.connect(
                 host=creds['host_name'],
                 user=creds['user_name'],
                 dbname=creds['db_name'],
                 password=creds['password'],
                 port=creds['port_number'])
            return connection

# -- Tworzenie tabel --
def setup_schema_and_data(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS użytkownicy (
        imie VARCHAR(40),
        nazwisko VARCHAR(40),
        adres_zamieszkania VARCHAR(40),
        PESEL CHAR(11) PRIMARY KEY,
        adres_mailowy VARCHAR(40),
        liczba_wypozyczonych_ksiazek SMALLINT CHECK (liczba_wypozyczonych_ksiazek BETWEEN 0 AND 15)
    );   
    """)
    conn.commit() 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS książki (
        nazwa_ksiazki VARCHAR(40),
        autor_ksiazki VARCHAR(40),
        indeks CHAR(13) PRIMARY KEY,
        pesel CHAR(11),
        czas_wypozyczenia SMALLINT CHECK (czas_wypozyczenia BETWEEN 0 AND 60),
        FOREIGN KEY (pesel) REFERENCES użytkownicy(PESEL) ON DELETE SET NULL
    );   
    """)
    conn.commit()
# -- Wczytywanie danych do tabeli użytkownicy z pliku csv --   
def load_users_from_csv(conn, csv_path):
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        cursor = conn.cursor()
        for row in reader:
            try:
                cursor.execute("""
                    INSERT INTO użytkownicy (imie, nazwisko, adres_zamieszkania, pesel, adres_mailowy, liczba_wypozyczonych_ksiazek)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    row['imię'],
                    row['nazwisko'],
                    row['adres zamieszkania'],
                    row['PESEL'],
                    row['adres mail'],
                    int(row['liczba wypożyczonych książek'])
                ))
                conn.commit()
            except Exception as e:
                print(f"Błąd przy dodawaniu rekordu: {e}")

# -- Wczytywanie danych do tabeli książki z pliku csv --
def load_books_from_csv(conn, csv_path):
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        cursor = conn.cursor()
        for row in reader:
            try:
                cursor.execute("""
                    INSERT INTO książki (nazwa_ksiazki, autor_ksiazki, indeks, pesel, czas_wypozyczenia)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    row['nazwa ksiązki'],
                    row['autor'],
                    row['indeks'],
                    row['przez kogo wypożyczona'] or None,
                    int(row['czas wypożyczenia'])
                ))
                conn.commit()
            except Exception as e:
                print(f"Błąd przy dodawaniu rekordu: {e}")


# -- Kopia zapasowa dla PostgreSQL --

def backup_database(conn):
    if DB_TYPE == 'postgres':
        !pg_dump -d student04db > student04db_backup.sql 


# -- Czyszczenie bazy --
def clear_tables(conn, tables):
    cursor = conn.cursor()
    for table in tables:
        cursor.execute(f'DROP TABLE IF EXISTS {table}')
    conn.commit()

# -- Przykładowe użycie --
if __name__ == "__main__":
    conn = connect_db()
    clear_tables(conn, [TABLE_NAME_2])
    clear_tables(conn, [TABLE_NAME_1])
    setup_schema_and_data(conn)
    load_users_from_csv(conn, '/home/student04/Lab8/Użytkownicy.csv')
    load_books_from_csv(conn, '/home/student04/Lab8/Książki.csv')
    backup_database(conn)
    conn.close()
