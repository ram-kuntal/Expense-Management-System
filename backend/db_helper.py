import mysql.connector
from contextlib import contextmanager
from logging_setup import setup_logger

logger = setup_logger('db_helper')

@contextmanager
def get_db_cursor(commit = False):
    connection = mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = "root",
        database = "expense_manager"
    )

    if connection.is_connected():
        print("Connection is successful")
    else:
        print("Connection Failed")

    cursor = connection.cursor(dictionary = True)
    yield cursor

    if commit:
        connection.commit()

    cursor.close()
    connection.close()


def fetch_expenses_for_date(expense_date):
    logger.info(f"fetch_expense_for_date called with {expense_date}")
    with get_db_cursor() as cursor:
        cursor.execute("SELECT * FROM expenses WHERE expense_date = %s", (expense_date,))
        expenses = cursor.fetchall()
        return expenses

def fetch_expenses_for_month():
    logger.info(f"fetch_expense_by_month")
    with get_db_cursor() as cursor:
        cursor.execute(
            '''SELECT month(expense_date) as expense_month,
               monthname(expense_date) as month_name,
               SUM(amount) as total FROM expenses
               GROUP BY expense_month, month_name;'''
        )
        data = cursor.fetchall()
        return data

def insert_expense(expense_date, amount, category, notes):
    logger.info(f"insert_expense called with {expense_date}")

    with get_db_cursor(commit = True) as cursor:
        cursor.execute(
            "INSERT INTO expenses(expense_date, amount, category, notes) VALUES(%s, %s, %s, %s)",
            (expense_date, amount, category, notes)
        )

def delete_expense_for_date(expense_date):
    logger.info(f"delete_expense_for_date called with {expense_date}")

    with get_db_cursor(commit = True) as cursor:
        cursor.execute("DELETE FROM expenses WHERE expense_date = %s", (expense_date,))

def fetch_expense_summary(start_date, end_date):
    logger.info(f"fetch_expense_summary called with start_date: {start_date} end_date: {end_date}")

    with get_db_cursor() as cursor:
        cursor.execute(
            '''SELECT category, SUM(amount) as total FROM expenses
             WHERE expense_date BETWEEN %s AND %s
             GROUP BY category;''',
            (start_date, end_date)
        )
        data = cursor.fetchall()
        return data


if __name__ == "__main__":
    expenses = fetch_expenses_for_date("2024-09-30")
    print(expenses)
    # delete_expense_for_date("2025-08-12")
    summary = fetch_expense_summary("2024-08-01", "2024-08-05")
    for record in summary:
        print(record)