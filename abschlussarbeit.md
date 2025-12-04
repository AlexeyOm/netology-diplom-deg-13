# Abschlussarbeit Data Engineer
##### Alexey Omelchenko

## Aufgabe

1. Entwickeln und dokumentieren Sie ETL-Prozesse zum Laden von Daten in ein Data Warehouse mit folgenden Schichten:
	- NDS (Normalized Data Warehouse) und DDS (Sternschema);
	- Datenqualität (optional, aber von großem Vorteil);
2. Erstellen Sie auf Basis des DDS Dashboards in Tableau.

### Ziel: Dokumentation der ETL-Prozesse anhand des vorgeschlagenen Datensatzes
1. Phasen der Fertigstellung der Arbeit
2. Datenverarbeitung und -analyse
3. Erstellung eines normalisierten Datenschemas (NDS)
4. Erstellung einer Zusammensetzung aus Fakten- und Dimensionstabellen (DDS)
5. Erstellung von ETL-Prozessen: zum Laden der Daten in das NDS und zur Erstellung von Data Marts
6. Erstellung von Metriken und Dashboards
7. Präsentation der Ergebnisse und Formulierung von Schlussfolgerungen

### Empfehlungen zur Durchführung der Arbeit:
1. ETL-Prozesse können wie folgt durchgeführt werden:
	- mit Pentaho;
	- mit Python (pandas) + SQL;
2. Datensatz:
	- wurde Ihnen oben im CSV-Format bereitgestellt;
	- Sie können auch Daten von Drittanbieter-APIs beziehen, was von Vorteil ist;
3. Zusätzlich können Sie die Orchestrierung mit Airflow durchführen;
4. Optional können Sie eine separate Metadatenebene im Speicher erstellen sowie Dashboards basierend auf den Daten dieser Ebene, die die Anzahl der Uploads und deren Status anzeigen;

### Ergebnis:
- Dashboards
- dokumentiertes Data-Warehouse-Schema
- dokumentiertes ETL-Prozessschema

Format: Diese Arbeit ist umfangreich. Wir empfehlen daher die Erstellung von Arbeitsmappen für die Verteidigung: Tableau, ERR-Diagramme für das Warehouse-Schema und KTR/KJB-Dateien mit ETL-Prozessen oder PY-Dateien mit Airflow-DAGs.

## Lösungsübersicht
	Die Lösung läuft auf den Cloud-Diensten der Google Cloud Platform.
	Google Bucket wird zur Speicherung der Eingabedaten und des Archivs verwendet.
	BigQuery dient zur Speicherung normalisierter Daten und des DSS.
	Dashboards werden mit Superset in der Google Kubernetes Engine erstellt.
	Die Cloud Composer-Orchestrierung basiert auf Airflow.
	Datenqualität: GreatExpectations.
	Die Daten stammen von https://www.mockaroo.com/; das Format entspricht dem Beispiel aus der Abschlussarbeit.

## Datenanalyse
Die Daten in der bereitgestellten CSV-Datei enthalten Verkaufsinformationen aus drei Städten in Myanmar über mehrere Monate mit insgesamt 1.000 Datensätzen. Alle Werte in allen Spalten sind vollständig ausgefüllt und enthalten keine Lücken oder ungültigen Werte. Die Datei enthält folgende Daten:

**Invoice ID** – Rechnungsnummer
**Branch** – Filiale (einer von drei Werten, kodiert durch einen einzelnen Buchstaben)
**City** – Ort (einer von drei Orten)
**Customer type** – Kundentyp (Mitgliedschaft in einem Treueprogramm)
**Gender** – Geschlecht des Kunden
**Product line** – Produktgruppe
**Unit price** – Einzelpreis
**Quantity** – Anzahl der gekauften Artikel
**Tax 5%** – 5 % Steuer (berechnet als 5 % des Einzelpreises multipliziert mit der gekauften Menge)
**Total** – Gesamtbetrag (Summe aus Preis der gekauften Artikel und 5 % Steuer)
**Date** – Kaufdatum
**Time** – Kaufzeit
**Payment** – Zahlungsart
**cogs** – Einkaufskosten
**gross margin percentage** – Einkaufsgewinn (fester Wert)
**gross income** – Gewinn
**Rating** – Bewertung

Alle Werte sind innerhalb der vorgegebenen Bereiche gleichmäßig verteilt. Die Generierung einer ähnlichen Datei und die Simulation der API zum Abrufen einer solchen Datei lassen sich mithilfe des Dienstes mockaroo.com problemlos realisieren. Weitere Details hierzu finden Sie im Abschnitt [Eingabedatengenerierung](https://github.com/AlexeyOm/netology-diplom-deg-13#%D0%B3%D0%B5%D0%BD%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D1%8F-%D1%82%D0%B5%D1%81%D1%82%D0%BE%D0%B2%D1%8B%D1%85-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85).


## Normale Datenform
Die folgenden Tabellen werden verwendet, um Daten in der dritten Normalform zu speichern:
#### sales_nf - Rechnungstabelle
	invoice_id STRING, -- Rechnungs-ID
	branch INT64, -- Filial-ID
	city INT64, -- Stadt-ID
	product_line INT64, -- Produktgruppen-ID
	payment_type INT64, -- Zahlungsmethoden-ID
	member_status INT64, -- Mitgliedschaftsstatus im Treueprogramm
	gender INT64, -- Geschlechts-ID
	transaction_time DATETIME, -- Transaktionszeitpunkt
	amount INT64, -- Menge des gekauften Produkts in Einheiten
	unit_price NUMERIC(8, 2), -- Einzelpreis
	cost NUMERIC(8, 2), -- Kaufkosten
	rating NUMERIC(2, 2) -- Kundenbewertung für den Kauf
#### cities - Städteverzeichnis
	city_id INT64 - Stadt-ID
	city_name STRING - Stadtname
#### branches - Verzeichnis der Filialen Abteilungen
	Branch_id INT64 – Abteilungs-ID
	Branch_name STRING – Abteilungsname
#### Zahlungsarten – Verzeichnis der Zahlungsarten
	payment_type_id INT64 – Zahlungsart-ID
	payment_type_name STRING – Name der Zahlungsart
#### Geschlechter – Verzeichnis der Geschlechter
	gender_id INT64 – Geschlechts-ID
	gender_name STRING – Geschlechtsname
#### Mitgliedsstatus – Verzeichnis der Mitgliedsstatus von Treueprogrammen
	member_status_id INT64 – Mitglieds-ID
	member_status_name STRING – Name des Mitgliedsstatus im Treueprogramm
#### Produktlinien – Verzeichnis der Produktgruppen
	member_status_id INT64 – Produktgruppen-ID
	member_status_name STRING – Name der Produktgruppe


Die Nachschlagewerke sind mit der Tabelle der Rechnungen durch eine **Eins-zu-viele-Beziehung** über Identifikatoren verknüpft.  
Einige Spalten aus der Datenquelle, wie **Tax 5%**, **Total**, **gross margin percentage**, **gross income**, sind nicht in die Normalform aufgenommen, da sie auf Basis der Felder **amount**, **unit_price** und **cost** berechnet werden.

#### fact_sales - Tabelle der Verkaufsfakten
    invoice_id string, -- Identifikator der Rechnung
    branch int64, -- Identifikator der Niederlassung
    city int64, -- Identifikator der Stadt
    product_line int64, -- Identifikator der Warengruppe
    payment_type int64, -- Identifikator des Zahlungsmittels
    member_status int64, -- Identifikator des Loyalitätsprogramm-Status
    gender int64, -- Identifikator des Geschlechts
    transaction_date string, -- Identifikator des Tages
    transaction_time string, -- Identifikator der Uhrzeit
    amount int64, -- Anzahl der gekauften Artikel in Stück
    unit_price numeric(8, 2), -- Preis pro Einheit
    cost numeric(8, 2), -- Selbstkosten des Kaufs
    rating numeric(4, 2) -- Bewertung des Kaufs durch den Kunden

Die SQL-Ausdrücke zur Erstellung der Tabellen in der Normalform sind in der [Datei](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/create_3nf_tables.sql) aufgeführt.

## Fakten- und Dimensionstabellen
Die Dimensionstabellen entsprechen größtenteils den Nachschlagetabellen der Normalform und werden gleichzeitig mit ihnen befüllt, da wir derzeit nicht über zusätzliche Daten verfügen, die diese Dimensionen anreichern könnten. Das heißt, alle Tabellen enthalten den Namen des Werts und die Wert-ID zur Verknüpfung mit der Faktentabelle.

Ausnahmen bilden die Datum-Dimensionstabelle und die Zeit-Dimensionstabelle.  
Für eine bequemere Analyse wurde die Datum-Dimension mit Feldern angereichert, die Informationen darüber enthalten, ob ein Tag ein Wochenend-, Feiertag- oder Werktag ist, den Namen des Feiertags (falls zutreffend), den Monatsnamen, die Monatsnummer sowie die Nummer des Tages und der Woche im Jahr.  
Für die Generierung der Datum-Dimension wurde die Tabelle *holidays* verwendet, die aus einer Spalte mit dem Feiertagsdatum und einer Spalte mit dem Feiertagsnamen besteht und manuell zusammengestellt wurde.

Die Zeit-Dimension wurde mit einem Merkmal angereichert, das angibt, zu welcher Tagesperiode jeder Wert gehört – Morgen, Tag oder Abend.

Das Gesamtschema der Tabellen und ihrer Beziehungen ist in der folgenden Abbildung dargestellt:  
![схема таблиц](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/images/netologyds%20-%20netologyds%20-%20sales.png)

## Generierung der Eingangsdaten
Die Zusammensetzung der erhaltenen Daten ist im Abschnitt [Datenanalyse](https://github.com/AlexeyOm/netology-diplom-deg-13#%D0%B0%D0%BD%D0%B0%D0%BB%D0%B8%D0%B7-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85) beschrieben.  
Zur Simulation und Generierung eines größeren Datenvolumens verwende ich den Dienst mockaroo.com, der die Möglichkeit bietet, Daten mit Hilfe von Standardfeldern wie Geschlecht, Datum, Uhrzeit sowie mit Formeln, Zufallszahlengeneratoren und Bedingungsoperatoren zu erzeugen.  
Die Beschreibung des Datenschemas ist unter folgendem [Link](https://www.mockaroo.com/07cd64d0) verfügbar.  

Auf Basis des Dienstes mockaroo wurde eine API erstellt, die unter der URL  
https://my.api.mockaroo.com/mock_sales_data.csv?key=78343830&date=2023-03-05  
verfügbar ist.  

Derzeit beeinflusst der Wert **date** in der Anfrage das Datum nicht, der Dienst liefert die Daten immer für den heutigen Tag.

Das Feld **date** wird im Format YYYY-MM-DD generiert, wobei immer das aktuelle Datum eingesetzt wird. In der vorherigen Version wurde der Wert des GET-Parameters der Anfrage verwendet, jedoch arbeitete der Dienst damit instabil.

Mit einer Wahrscheinlichkeit von 1 % werden die Daten in den Spalten **Customer type**, **Gender**, **Product line**, **Date**, **Time** leer gelassen, um fehlerhafte bzw. unvollständige Daten zu simulieren.

Die Daten werden über die API im CSV-Format geladen, der Trenner ist ein Komma, und die Spaltenüberschriften befinden sich in der ersten Zeile.


## ETL-Prozesse
Die Verarbeitung der Daten erfolgt nach folgendem Ablauf:

1. Laden der Ausgangsdatei – Aufruf des Dienstprogramms **curl** mittels Airflow **BashOperator**  
2. Überprüfung der Datei mit dem Paket **Great Expectations**, Details im Abschnitt [Datenqualität](https://github.com/AlexeyOm/netology-diplom-deg-13#%D0%BA%D0%B0%D1%87%D0%B5%D1%81%D1%82%D0%B2%D0%BE-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85)
3. Herausfiltern ungültiger Daten und Speichern gültiger Daten in der Datenbank im Rohformat – [Python-Skript](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/dags/load_valid_to_bq_callable.py)
4. Schreiben von Informationen über nicht bestandene Validierungen in eine BigQuery-Tabelle zur manuellen Fehleranalyse – Great-Expectations-Plugin, [Python-Skript](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/plugins/write_results_to_bq.py)
5. Prüfung, ob in den qualitätsgeprüften Daten neue Werte für Nachschlagetabellen und Dimensionen enthalten sind, und Ergänzung dieser Tabellen falls nötig – [BigQuery-Stored Procedure](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/lookup_update_procedure.sql)
6. Schreiben der validierten Daten in Normalform in BigQuery-Tabellen – [BigQuery-Stored Procedure](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/copy_raw_to_3nf.sql)
7. Schreiben der validierten Daten in die Faktentabellen – [BigQuery-Stored Procedure](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/copy_raw_to_fact.sql)
8. Verschieben der CSV-Datei in das Archiv – Airflow-Operator **LocalFilesystemToGCSOperator**


## Orchestrierung
Die Orchestrierung wurde mit dem Dienst **Composer GCP** auf Basis von **Apache Airflow** umgesetzt.  
Das Abrufen, Transformieren und Laden der Daten erfolgt durch den täglichen Start des DAG um 23:00 Uhr:  
[daily_etl.py](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/dags/daily_etl.py)

Alle Schritte des ETL-Prozesses werden nacheinander ausgeführt.  
Ein Versuch, einzelne Prozesse parallel auszuführen – zum Beispiel den Vorgang zur Aktualisierung der sechs Nachschlagetabellen – führt zu Warnmeldungen und sogar zu Fehlern beim Laden von DAG-Aufgaben.  
Dies hängt mit den finanziellen Beschränkungen des Lernprojekts zusammen: Für Composer wurde die minimale virtuelle Maschinenkonfiguration gewählt.

DAG in der Airflow-Oberfläche  
![DAG in der Airflow-Oberfläche](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/images/dag.png)

## Datenqualität
Die Überprüfung der Datenqualität erfolgt mit dem Paket [Great Expectations](https://greatexpectations.io/), das es ermöglicht, Anforderungen an die Daten deklarativ festzulegen, die Quelle der zu prüfenden Daten zu beschreiben und Maßnahmen zu definieren, die nach der Prüfung ausgeführt werden.  
Als Datenquelle können sowohl Tabellen in Datenbanken als auch Dateien dienen; im vorliegenden Projekt wird die zweite Variante verwendet.  
Die Konfigurationen werden in YML-Dateien gespeichert:

[great_expectations.yml](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/great_expectations.yml)

[production_checkpoint.yml](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/checkpoints/production_checkpoint.yml)

Die Anforderungen an die Datenqualität, sogenannte *Expectations*, werden in der Datei  
[sales_data_expectations.json](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/expectations/sales_data_expectations.json) gespeichert.

Die Erwartungen können sich sowohl auf den Datentyp, die Anzahl der Spalten als auch auf zulässige Wertebereiche einzelner Spalten beziehen.  
In komplexeren Fällen ist es möglich, Erwartungen zum Verteilungstyp der Messgrößen in bestimmten Spalten zu definieren. Beispielsweise kann für einige Werte eine Normalverteilung erwartet werden; Abweichungen davon würden auf eine untypische Situation hinweisen, die ein Eingreifen erforderlich macht.

Im Lernbeispiel werden folgende Aspekte geprüft:  
– Vorhandensein aller erforderlichen Spalten,  
– keine fehlenden Werte,  
– Übereinstimmung des invoice_id mit einem regulären Ausdruck,  
– Grenzwerte für numerische Spalten.

Ein Beispiel für eine Erwartung zur Übereinstimmung mit einem regulären Ausdruck sieht folgendermaßen aus:

    {
      "expectation_type": "expect_column_values_to_match_regex",
      "kwargs": {
        "column": "Invoice ID",
        "mostly": 1.0,
        "regex": "^\\d\\d\\d-\\d\\d-\\d\\d\\d\\d$"
      },
      "meta": {}
    }

Beim Ausführen des Pakets über ein Bash-Skript als Airflow-Task wird die CSV-Datei auf die Einhaltung der Erwartungen überprüft.  
Als Ergebnis der Prüfung wird eine JSON-Datei erzeugt und in einen GCP-Bucket geschrieben.  
Zusätzlich werden übersichtliche HTML-Dateien generiert, die von einem Data-Quality-Spezialisten eingesehen werden können, um die Situation schnell zu beurteilen und Entscheidungen zum Umgang mit fehlerhaften Daten zu treffen.  

Die Ergebnisse der Ausführungen sind unter folgendem [Link](https://storage.googleapis.com/sample-sales-23/index.html) verfügbar.  
Da die Speicherung der Daten kostenpflichtig ist – wenn auch nur in geringem Umfang – kann der Zugriff nicht garantiert werden.

Auf Basis der JSON-Datei mit den Prüfergebnissen schreibt das Plugin, das als  
[Python-Skript](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/gx/plugins/write_results_to_bq.py) implementiert ist, die Informationen über die Ergebnisse in die Tabelle **data_loading_log**,  
die Metadaten zur Datenqualität enthält und im Dashboard verfügbar ist.

## Datenvisualisierung
Die Visualisierung erfolgt mit dem Paket **Apache Superset**, das in **Google Kubernetes Engine** bereitgestellt wurde.  
Die Dashboards sind unter folgendem [Link](http://34.111.226.84/login/) verfügbar.

Für die Darstellung der in der **Sternschema**-Struktur gespeicherten Daten wird ein flaches View verwendet, das mit folgendem  
[SQL-Ausdruck](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/sql/create_flat_view.sql) erstellt wurde.

In Apache Superset wurden zwei Dashboards erstellt – eines zeigt die Situation der Verkaufsdaten und Kundeninformationen für den ausgewählten Zeitraum, das andere visualisiert die Daten der Metaschicht, die für die Datenqualität verantwortlich ist.

#### Dashboard mit Verkaufs- und Kundendaten
![дашборд с данными по продажам и покупателям](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/images/mt-2023-03-23T20-00-23.318Z.jpg)  

#### Dashboard zur Datenqualität
![дашборд по качеству данных](https://github.com/AlexeyOm/netology-diplom-deg-13/blob/main/images/mt-2023-03-23T20-00-40.763Z.jpg)


## Kosten der Cloud-Lösung
Die Kosten der Cloud-Lösung bestehen aus zwei Komponenten, die bei der aktuellen Datenmenge bezahlt werden müssen: **Google Compute** und **Computation Engine**.  
Die durchschnittlichen täglichen Ausgaben betrugen jeweils 12,75 und 3,7 US-Dollar, was einen durchschnittlichen Gesamtbetrag von 16,45 US-Dollar für den Entwicklungszeitraum ergibt.  
Unter Berücksichtigung der 300 US-Dollar an GCP-Guthaben können damit 19 Entwicklungstage abgedeckt werden.

## Fazit
Die Abschlussarbeit für den Beruf *Data Engineer* wurde mit dem Ziel erstellt, das im Kurs „Data Engineer“ erworbene Wissen und die praktischen Fähigkeiten zu konsolidieren.

Im Rahmen der Arbeit wurden folgende Schritte durchgeführt:

- Verarbeitung und Analyse der Daten  
- Erstellung eines normalisierten Datenschemas (NDS)  
- Bildung der Tabellen für Fakten und Dimensionen (DDS)  
- Entwicklung von ETL-Prozessen zum Laden der Daten in das NDS und zur Erstellung von Data Marts  
- Erstellung eines Satzes von Metriken und Dashboards auf deren Grundlage

