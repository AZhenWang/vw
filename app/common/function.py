def str2hump(text):
    """下划线转驼峰法

    :param text: 下划线字符串
    :return: 驼峰法字符串
    """
    arr = filter(None, text.lower().split('_'))
    res = ''
    for i in arr:
        res = res + i[0].upper() + i[1:]
    return res

def combine_cols(columns=[]):
    """combine columns orderly

    :param columns:
    :return:
    """
    def combine(l, n):
        answers = []
        one = [0] * n

        def next_c(li=0, ni=0):
            if ni == n:
                answers.append(one.copy())
                return
            for lj in range(li, len(l)):
                one[ni] = l[lj]
                next_c(lj + 1, ni + 1)

        next_c()
        return answers

    combined_cols_set = []
    length = len(columns)

    for i in range(length, 0, -1):
        combined_cols_set.append(combine(columns, i))

    return combined_cols_set