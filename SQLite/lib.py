import sqlite3
import json

def jsonToLite(dbPath, jsonPath, tabName):
    # Mapowanie nazw z JSON-a na kolumny w SQLite
    field_map = {
        "imię": "imie",
        "nazwisko": "nazwisko",
        "adres zamieszkania": "adres_zamieszkania",
        "PESEL": "PESEL",
        "adres mail": "adres_mailowy",
        "liczba wypożyczonych książek": "liczba_wypozyczonych_ksiazek"
    }

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

def jsonToLite2(dbPath, jsonPath, tabName):
    # Mapowanie nazw z JSON-a na kolumny w SQLite
    field_map = {
        "nazwa ksiązki": "nazwa_ksiazki",
        "autor": "autor_ksiazki",
        "indeks": "indeks",
        "przez kogo wypożyczona": "pesel",
        "czas wypożyczenia": "czas_wypozyczenia"
    }
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
    
    conn1 = sqlite3.connect('folder/litedata.db')
    backcon = sqlite3.connect('folder/backup_litedata.db')
    backcon.backup(conn1)
    backcon.commit()
    conn1.commit()
    backcon.close()
    conn1.close()
    print("Database restored successfully")
    return
