from backend import db_helper

def test_fetch_expenses_for_date_aug_15():
    expenses = db_helper.fetch_expenses_for_date("2024-08-15")
    assert len(expenses) == 1
    assert expenses[0]['amount'] == 10.0
    assert expenses[0]['category'] == "Shopping"
    assert expenses[0]['notes'] == "Bought potatoes"


def test_fetch_expenses_for_date_invalid_date():
    expenses = db_helper.fetch_expenses_for_date("9999-08-15")
    assert len(expenses) == 0


def test_fetch_expense_summary():
    summary = db_helper.fetch_expense_summary("2024-08-01", "2024-08-05")
    assert len(summary) == 5

def test_fetch_expense_summary_invalid_range():
    summary = db_helper.fetch_expense_summary("2025-08-01", "2026-08-05")
    assert len(summary) == 0