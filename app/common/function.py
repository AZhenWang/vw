def str2hump(text):
    arr = filter(None, text.lower().split('_'))
    res = ''
    for i in arr:
        res = res + i[0].upper() + i[1:]
    return res