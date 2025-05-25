def group_by(arr: list, k):
    d = dict()
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
