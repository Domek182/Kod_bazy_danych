import sqlite3
import json

def connect_db():
    """
    Nawiązuje połączenie z bazą danych SQLite.

    :return: Obiekt połączenia z bazą danych `litedata.db` znajdującą się w folderze `folder/`.
    :rtype: sqlite3.Connection
    """
    connection = sqlite3.connect('folder/litedata.db')
    return connection

def setup_schema(conn):
    """
    Tworzy schemat bazy danych SQLite, jeśli jeszcze nie istnieje.

    Funkcja tworzy dwie tabele:
    - **użytkownicy**: zawiera dane osobowe użytkowników oraz liczbę wypożyczonych książek.
    - **książki**: zawiera dane o książkach oraz informację, kto je wypożyczył.

    W tabeli `książki` klucz obcy `pesel` odwołuje się do tabeli `użytkownicy`, a usunięcie użytkownika ustawia wartość `NULL`.

    :param conn: Obiekt połączenia z bazą danych SQLite.
    :type conn: sqlite3.Connection

    :return: None
    :rtype: None
    """
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS użytkownicy (
        imie TEXT,
        nazwisko TEXT,
        adres_zamieszkania TEXT,
        PESEL TEXT PRIMARY KEY,
        adres_mailowy TEXT,
        liczba_wypozyczonych_ksiazek INTEGER CHECK (liczba_wypozyczonych_ksiazek BETWEEN 0 AND 15)
    );
    """)
    conn.commit()
    c.execute("PRAGMA foreign_keys = ON;")
    c.execute("""
        CREATE TABLE IF NOT EXISTS książki (
        nazwa_ksiazki TEXT,
        autor_ksiazki TEXT,
        indeks TEXT PRIMARY KEY,
        pesel TEXT,
        czas_wypozyczenia INTEGER CHECK (czas_wypozyczenia BETWEEN 0 AND 60),
        FOREIGN KEY (pesel) REFERENCES użytkownicy(PESEL) ON DELETE SET NULL
    ); 
    """)
    conn.commit()
   
def jsonToLite(dbPath, jsonPath, tabName, field_map):
    """
    Wczytuje dane z pliku JSON i zapisuje je do wskazanej tabeli w bazie SQLite.

    Funkcja:
    - Łączy się z bazą danych SQLite.
    - Odczytuje dane z pliku JSON.
    - Dopasowuje klucz odpowiadający nazwie tabeli (ignorując wielkość liter).
    - Mapuje pola z JSON-a na kolumny tabeli zgodnie z przekazaną mapą `field_map`.
    - Wstawia dane do tabeli za pomocą zapytania `INSERT`.

    Obsługuje błędy połączenia z bazą, odczytu pliku oraz wstawiania danych.

    :param dbPath: Ścieżka do pliku bazy danych SQLite.
    :type dbPath: str
    :param jsonPath: Ścieżka do pliku JSON zawierającego dane.
    :type jsonPath: str
    :param tabName: Nazwa tabeli w bazie danych, do której mają zostać zapisane dane.
    :type tabName: str
    :param field_map: Słownik mapujący klucze z JSON-a na nazwy kolumn w tabeli.
    :type field_map: dict[str, str]

    :return: None
    :rtype: None
    """
  
    try:
        conn = sqlite3.connect(dbPath)
        cursor = conn.cursor()
    except Exception as e:
        print("Błąd połączenia z bazą danych:", e)
        return

    try:
        with open(jsonPath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print("Błąd przy otwieraniu pliku JSON:", e)
        conn.close()
        return

    try:
        # Dopasowanie klucza niezależnie od wielkości liter
        key = next((k for k in data if k.lower() == tabName.lower()), None)
        if not key:
            print(f"Nie znaleziono danych dla klucza '{tabName}' w pliku JSON.")
            conn.close()
            return

        records = data[key]
        if not records:
            print("Brak danych do wstawienia.")
            conn.close()
            return

        # Przygotowanie zapytania SQL
        columns = [field_map[k] for k in field_map]
        placeholders = ', '.join(['?'] * len(columns))
        column_names = ', '.join(columns)
        sql = f"INSERT INTO {tabName} ({column_names}) VALUES ({placeholders})"

        # Wstawianie danych
        for record in records:
            values = tuple(record[k] for k in field_map)
            cursor.execute(sql, values)

        conn.commit()
        print(f"Wstawiono {len(records)} rekordów do tabeli '{tabName}'.")
    except Exception as e:
        print("Błąd podczas wprowadzania danych:", e)
    finally:
        conn.close()

def backup():
    """
    Tworzy kopię zapasową bazy danych SQLite.

    Funkcja:
    - Łączy się z główną bazą danych `litedata.db`.
    - Tworzy nową bazę `backup_litedata.db` w tym samym folderze.
    - Kopiuje całą zawartość głównej bazy do pliku kopii zapasowej.
    - Zamyka połączenia i wypisuje komunikat o powodzeniu operacji.

    :return: None
    :rtype: None
    """
    conn1 = sqlite3.connect('folder/litedata.db')
    backcon = sqlite3.connect('folder/backup_litedata.db')
    conn1.backup(backcon)
    conn1.commit()
    backcon.commit()
    backcon.close()
    conn1.close()
    print("Database backup successful")
    return

def dropTable(tabname):
    """
    Usuwa wskazaną tabelę z bazy danych SQLite.

    Funkcja:
    - Łączy się z bazą danych `litedata.db`.
    - Próbuje usunąć tabelę o nazwie `tabname` za pomocą polecenia `DROP TABLE`.
    - Obsługuje błędy związane z nieistniejącą tabelą lub nieprawidłową nazwą.
    - Zamyka połączenie z bazą danych po zakończeniu operacji.

    :param tabname: Nazwa tabeli do usunięcia.
    :type tabname: str

    :return: None
    :rtype: None
    """
    
    conn1 = sqlite3.connect('folder/litedata.db')
    c = conn1.cursor()
    try:
        c.execute("""DROP TABLE %s;"""%(tabname))
        print("tabela usunięta")
    except:
        print("błąd podczas usuwania tabeli")
    conn1.commit()
    conn1.close()
    
    return

def restore():
    """
    Przywraca główną bazę danych z kopii zapasowej.

    Funkcja:
    - Łączy się z kopią zapasową `backup_litedata.db` oraz główną bazą `litedata.db`.
    - Kopiuje zawartość kopii zapasowej do głównej bazy danych.
    - Zamyka połączenia i wypisuje komunikat o powodzeniu operacji.

    :return: None
    :rtype: None
    """
    
    conn1 = sqlite3.connect('folder/litedata.db')
    backcon = sqlite3.connect('folder/backup_litedata.db')
    backcon.backup(conn1)
    backcon.commit()
    conn1.commit()
    backcon.close()
    conn1.close()
    print("Database restored successfully")
    return
