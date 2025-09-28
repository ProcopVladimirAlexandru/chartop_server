import datetime


def group_by(arr: list, k: str):
    d: dict = dict()
    for el in arr:
        uid = None
        try:
            uid = el[k]
        except Exception:
            pass
        if uid is None:
            uid = getattr(el, k, None)
        if uid is None:
            continue
        if uid in d:
            d[uid].append(el)
        else:
            d[uid] = [el]
    return d


def get_now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)
