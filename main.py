import csv
from sqlalchemy import create_engine, Table, Column, Integer, MetaData, Float, String, text


def detect_type(value):
    if value.isdigit():
        return Integer
    else:
        try:
            float(value)
            return Float
        except ValueError:
            return String


def csv_to_sqlite(database_instance, csvfile, table_name: str):
    metadata = MetaData()
    with open(csvfile, 'r') as file:
        reader = csv.DictReader(f=file)
        data = list(reader)

        reader_dict = dict(data[0])
        headers_name = list(reader_dict.keys())
        columns = []

        for col_name, value in zip(headers_name, reader_dict.values()):
            col_type = detect_type(value)
            columns.append(Column(col_name, col_type))

        table = Table(table_name, metadata, *columns)
        metadata.create_all(database_instance)

        connection = database_instance.connect()
        for row in data:
            insert_stmt = table.insert().values(dict(zip(headers_name, row.values())))
            connection.execute(insert_stmt)

        connection.commit()
        connection.close()


engine = create_engine('sqlite:///sqlalchemy.db')
file_list = [{"file_name": "clean_stations.csv", "table_name": "stations"},
             {"file_name": "clean_measure.csv", "table_name": "measures"}]

for file in file_list:
    csv_to_sqlite(database_instance=engine, csvfile=file["file_name"], table_name=file["table_name"])

with engine.connect() as conn:
    query = text("SELECT * FROM stations LIMIT 5")
    result = conn.execute(query).fetchall()

    for row in result:
        print(row)
