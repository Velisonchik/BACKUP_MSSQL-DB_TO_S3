import pymssql
from datetime import datetime
from DB_req import *
from file_uploader import upload_bak


def write_log(text):
    with open('log.log', 'a') as log:
        current_datetime = datetime.now()
        log.write(f'{current_datetime} - {text}\n')


def main():
    try:
        connection = pymssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DB, autocommit=True)
        sql = f"BACKUP DATABASE [{DB}] TO DISK = N'{backup_file}'"
        cursor = connection.cursor().execute(sql)
        connection.close()
        write_log('Backup created successfully')
        return True
    except Exception as e:
        write_log(e)
        return False


if __name__ == '__main__':
    current_date = datetime.now().date()
    backup_file = f"C:\\Backup\\rdm_{current_date}.bak"
    if main():
        upload_bak(f'rdm{current_date}.bak', backup_file)
