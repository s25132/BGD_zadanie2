# BGD_zadanie1

## Cel zadania 
Celem projektu jest zaprojektowanie i implementacja skalowalnego pipeline’u przetwarzania danych transakcyjnych, który przekształca surowe, potencjalnie błędne dane w wysokiej jakości model analityczny (warstwa GOLD), umożliwiający wiarygodne raportowanie i analizę biznesową.

## Dane
Dane posidają następujące kolumny 

- transaction_id – unikalny identyfikator transakcji
- customer_id – unikalny identyfikator klienta
- customer_name – imię i nazwisko klienta
- merchant_id – unikalny identyfikator sprzedawcy
- transaction_ts – znacznik czasu wykonania transakcji
- amount – kwota transakcji
- city – miasto sprzedawcy
- country – kraj sprzedawcy
- payment_method – metoda płatności
- status – status transakcji

Do repozytorium GitHub dołączone zostały przykładowe pliki danych (katalog data).
Pipeline był również testowany na dużym pliku CSV o rozmiarze ~2,5 GB (https://drive.google.com/file/d/1uI-XWg8u_DqIr5-i24zRgWTYXB_m1Kfj/view?usp=sharing).

Problemy z danymi: 
- czasami null w kolumnie transaction_id
- błędna wartośc w kolumnie amount 
- błędna wartośc w kolumnie transaction_ts 
- duże i małe wyrazy określające ten sam status statuses = ["approved", "declined", "pending", "APPROVED"] 
- duże i małe wyrazy określające tą samą metodę płatności payment_methods = ["card", "blik", "transfer", "cash", "CARD"]

## Pipeline 

Pipeline ma cztery moduły
###  RAW Ingestion – raw.py

Cel: przechowywanie surowych, historycznych danych (append-only)

- Wczytuje dane z pliku CSV w partiach (chunkach)
- Oblicza hash pliku i sprawdza, czy ten sam plik (nazwa + hash) został już wcześniej załadowany
- Ładuje tylko nowe pliki, dzięki czemu unika ponownego przetwarzania tych samych danych
- Nadaje każdej partii danych numer batcha (batch_no)
- Dodaje metadane: source_file, file_hash, loaded_at
- Zapisuje dane do tabeli: raw.transactions_raw
- Rejestruje załadowany plik w tabeli: raw.ingestion_log

Cechy:

- append-only (brak kasowania danych)
- pełna historia danych
- możliwość audytu i ponownego przetwarzania

### Cleaned and validated data – silver.py

Cel: oczyszczone i zwalidowane dane

- Pobiera dane z warstwy RAW
- Przetwarza tylko nowe batch’e (na podstawie silver.batch_log)
- Czyści i normalizuje dane (tekst, daty, liczby)
- Wykonuje walidację danych (np. brak transaction_id, błędna data, błędna kwota, ujemna kwota)
- Dodaje kolumny: is_valid oraz validation_error
- Zapisuje dane do: silver.transactions_clean
- Używa mechanizmu UPSERT (ON CONFLICT DO UPDATE) na kluczu transaction_id
- Rejestruje przetworzony batch w tabeli: silver.batch_log

Cechy:

- przetwarzanie inkrementalne (tylko nowe batch’e)
- idempotentność (wielokrotne uruchomienie daje ten sam wynik)
- brak duplikatów na poziomie transaction_id

### Analytical Modeling – gold.py 

Cel: dane gotowe do analizy i raportowania (schemat gwiazdy)

Buduje model analityczny:

- Tabele wymiarów:
    - gold.dim_customer
    - gold.dim_merchant
    - gold.dim_date
- Tabela faktów:
    - gold.fact_transactions
- Widok:
    - gold.v_transaction_report

Logika działania:

- Uwzględnia tylko poprawne dane (is_valid = true)
- Dla tabel wymiarów wybiera jeden rekord na klucz (np. customer_id)
- Stosuje deduplikację (np. DISTINCT ON), aby uniknąć wielu wersji tego samego rekordu
- Wszystkie tabele ładowane są przez UPSERT (ON CONFLICT DO UPDATE)
- Tabela faktów zawiera jeden rekord na transaction_id

Cechy:

- brak duplikatów w wymiarach
- możliwość aktualizacji danych (np. zmiana nazwy klienta)
- model typu star schema

### Pipeline control - pipeline.py

- steruje uruchamianiem wszystkich etapów przetwarzania danych (RAW, SILVER, GOLD).

## Uruchomienie

Uruchomienie skrytpu pipeline 

docker compose -f pipeline_docker.yml build 

docker compose -f pipeline_docker.yml up 

Podgląd danych 

docker compose -f show_data.yml build 

docker compose -f show_data.yml up

## Sql tworzący bazę danych
BGD_zadanie1/pipeline/db/init.sql

## Przydatne sql
SELECT count(1) FROM "raw".transactions_raw

SELECT count(1) FROM silver.transactions_clean

select * from gold.fact_transactions limit 1000


SELECT 
    schemaname,
    relname AS table_name,
    pg_total_relation_size(relid) / 1024 / 1024 / 1024 AS size_gb,
	pg_total_relation_size(relid) / 1024 / 1024 AS mb_gb
FROM pg_catalog.pg_statio_user_tables
ORDER BY size_gb DESC

## Architektura
![GRAPH](BGD_zadanie11.png)