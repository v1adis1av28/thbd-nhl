"""Медленно меняющееся измерение, тип 2.

Новая версия открывается, когда меняется отслеживаемый атрибут.
Очки и счёт сюда не входят: это факты, а не свойства сущности.
"""

from __future__ import annotations


def build_scd2(rows: list[dict], natural_key: str, tracked: list[str], date_field: str) -> list[dict]:
    grouped: dict = {}
    for row in rows:
        if row.get(natural_key) is None or row.get(date_field) is None:
            continue
        grouped.setdefault(row[natural_key], []).append(row)

    versions: list[dict] = []
    for items in grouped.values():
        items.sort(key=lambda item: item[date_field])
        opened: dict | None = None
        for item in items:
            signature = tuple(item.get(field) for field in tracked)
            if opened is None:
                opened = {**item, "valid_from": item[date_field], "_signature": signature}
                continue
            if signature == opened["_signature"]:
                continue
            opened["valid_to"] = item[date_field]
            opened["is_current"] = False
            versions.append(opened)
            opened = {**item, "valid_from": item[date_field], "_signature": signature}
        if opened is not None:
            opened["valid_to"] = None
            opened["is_current"] = True
            versions.append(opened)

    for version in versions:
        version.pop("_signature", None)
    return versions


def sk_on_date(versions: list[dict], on_date: str, sk_field: str) -> int | None:
    for version in versions:
        start = version["valid_from"]
        end = version["valid_to"]
        if start <= on_date and (end is None or on_date < end):
            return version[sk_field]
    return None
