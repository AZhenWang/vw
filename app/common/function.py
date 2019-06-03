import numpy as np

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


def get_cum_return(prices, holdings=[]):
    """
    组装累积收益率
    :param prices:
    :param holdings:
    :return:
    """
    cum_return_set = [0] * 2
    cum_return = 0
    return_from_entry = 0
    buy_price = 0
    fee_rate = 0.004  # include tax rate and brokerage charges
    for i in range(2, len(holdings)):

        if holdings[i - 1] == 1 and holdings[i - 2] != 1:
            buy_price = prices.iloc[i]
            cum_return -= fee_rate

        if holdings[i - 1] == 0 and holdings[i - 2] != 0 and buy_price != 0:
            cum_return -= fee_rate * prices.iloc[i] / buy_price

        if holdings[i - 1] == 1:
            sell_price = prices.iloc[i]
            diff = sell_price - buy_price
            return_from_entry = diff / min([sell_price, buy_price])

        cum_return_set.append(cum_return + return_from_entry)

        if holdings[i - 1] == 0 and holdings[i - 2] == 1:
            cum_return += return_from_entry
            return_from_entry = 0

    return cum_return_set


def get_cum_return_rate(prices, holdings=[]):
    """
    组装累积收益率
    :param prices:
    :param holdings:
    :return:
    """
    cum_return_rate_set = [0] * 2
    cum_return_rate = 0
    fee_rate = 0.4  # include tax rate and brokerage charges
    for i in range(2, len(holdings)):

        if holdings[i - 1] != holdings[i - 2]:
            cum_return_rate -= fee_rate

        if holdings[i - 1] == 1:
            diff = prices.iloc[i] - prices.iloc[i-1]
            cum_return_rate += diff / min([prices.iloc[i], prices.iloc[i-1]]) * 100

        cum_return_rate_set.append(cum_return_rate)

    return cum_return_rate_set


def get_buy_sell_points(holdings):
    buy, sell = [np.nan], [np.nan]
    for i in range(1, len(holdings)):
        if holdings[i] != holdings[i - 1]:
            if holdings[i] == 1:
                buy.append(1)
                sell.append(np.nan)
            else:
                sell.append(1)
                buy.append(np.nan)
        else:
            buy.append(np.nan)
            sell.append(np.nan)

    return buy, sell


def pack_daily_return(prices):
    """
    组装自定义每日收益率
    :param prices:
    :return:
    """
    pre_prices = prices.shift(1)
    diff = prices - pre_prices
    daily_return = diff / np.min([prices, pre_prices], axis=0) * 100

    return daily_return


def send_sms(text):
    """
    发送短信
    :param text:
    :return:
    """
    from twilio.rest import Client
    from conf.myapp import twilio

    client = Client(twilio['account_sid'], twilio['auth_token'])

    message = client.messages.create(
        body=text,
        from_='+19159955943',
        to='+8613267210646'
    )
    return message


def send_email(subject='趋势预测', msgs=[]):
    """
    发送邮件
    :param subject:
    :param msgs:
    :return:
    """
    import smtplib

    from email.header import Header
    from email.mime.multipart import MIMEMultipart

    # 第三方 SMTP 服务
    mail_host = "smtp.qq.com"  # 设置服务器
    mail_user = "865820954@qq.com"  # 用户名
    mail_pass = "mfhgotjzrvmabdeb"  # 口令

    sender = '865820954@qq.com'
    receivers = ['865820954@qq.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    # receivers = ['865820954@qq.com', '535362768@qq.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    outer = MIMEMultipart()
    outer['From'] = Header("蓬头小龙虾", 'utf-8')
    outer['Subject'] = Header(subject, 'utf-8')
    for msg in msgs:
        outer.attach(msg)

    try:
        smtpObj = smtplib.SMTP_SSL(mail_host, 465)

        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, outer.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")


def knn_predict(X, Y, k, sample_interval, pre_predict_interval, predict_idx):
    """predict one date by knn algorithm

    Args:
        X: normalized features with DataFrame type
        Y: datetime.date object, or a string with 'year-month-day' format
        sample_interval: 预测间隔
        pre_predict_interval: 向前计算多少天的涨跌幅
        predict_idx: the index to predict

    Returns:
        y_hat: the value predicted by knn algorithm

    """
    import numpy as np
    from sklearn.neighbors import KNeighborsRegressor

    try:
        predict_iloc = Y.index.get_loc(predict_idx)
    except:
        return np.nan
    if predict_iloc < sample_interval:
        sample_start_iloc = 0
    else:
        sample_start_iloc = predict_iloc - sample_interval

    sample_end_iloc = predict_iloc - pre_predict_interval + 1
    training_X = X.iloc[sample_start_iloc:sample_end_iloc]
    training_Y = Y.iloc[sample_start_iloc:sample_end_iloc]
    testing_x = X.iloc[predict_iloc]
    knn = KNeighborsRegressor(n_neighbors=k)
    knn.fit(training_X, training_Y)
    predicted_Y = knn.predict([testing_x])
    y_hat = predicted_Y[0]
    return y_hat


def remove_noise(Y, unit=0.05):
    """
    去除噪音
    :param Y: series
    :param unit:
    :return:
    """

    if len(Y) < 3:
        return Y
    return Y
    Y_diff = np.diff(Y)
    w = np.argwhere(abs(Y_diff) < unit)
    max_key = Y.keys()[-1]
    for n in w:
        n = n[0]
        if n < max_key - 1 and (Y_diff[n] * Y_diff[n+1] < 0):
            Y_hat.iloc[n] = (Y.iloc[n-1] + Y.iloc[n + 1]) / 2
    return Y_hat


def get_wave_segment(Y):
    """
    获取波浪阶段
    :param Y: Series
    :return:
    """
    point_args = np.diff(np.where(np.diff(Y) > 0, 0, 1))
    peaks = Y[1:-1][point_args == 1]
    bottoms = Y[1:-1][point_args == -1]
    qqb = 0
    if len(peaks) < 4 or len(bottoms) < 4:
        return qqb

    if peaks.iloc[-1] < peaks.iloc[-2] < peaks.iloc[-3] < peaks.iloc[-4] \
            and bottoms.iloc[-1] < bottoms.iloc[-2] < bottoms.iloc[-3]:
        qqb = -7
    elif Y.iloc[-2] < Y.iloc[-1] < peaks.iloc[-1] < peaks.iloc[-2] < peaks.iloc[-3] \
            and Y.iloc[-1] < bottoms.iloc[-1] < bottoms.iloc[-2] < bottoms.iloc[-3]:
        qqb = -6
    elif peaks.iloc[-1] < peaks.iloc[-2] < peaks.iloc[-3] \
            and bottoms.iloc[-1] < bottoms.iloc[-2] \
            and Y.iloc[-1] < Y.iloc[-2]:
        qqb = -5
    elif peaks.iloc[-1] < peaks.iloc[-2] \
            and bottoms.iloc[-1] < bottoms.iloc[-2] \
            and Y.iloc[-2] < Y.iloc[-1] < bottoms.iloc[-2]:
        qqb = -4
    elif peaks.iloc[-1] > peaks.iloc[-2]\
            and bottoms.iloc[-2] < bottoms.iloc[-3] < bottoms.iloc[-1] \
            and Y.iloc[-1] > Y.iloc[-2]:
        qqb = 3
    elif peaks.iloc[-1] > peaks.iloc[-2] > peaks.iloc[-3]  \
            and bottoms.iloc[-1] > bottoms.iloc[-2] > bottoms.iloc[-3] \
            and Y.iloc[-1] < Y.iloc[-2]:
        qqb = 6
    elif peaks.iloc[-3] > peaks.iloc[-4] \
            and peaks.iloc[-1] < peaks.iloc[-2] \
            and bottoms.iloc[-2] > bottoms.iloc[-3] > bottoms.iloc[-4] \
            and Y.iloc[-1] > Y.iloc[-2]:
        qqb = 7
    elif peaks.iloc[-1] > peaks.iloc[-2] > peaks.iloc[-3] \
            and bottoms.iloc[-1] > bottoms.iloc[-2] > bottoms.iloc[-3] \
            and Y.iloc[-1] > Y.iloc[-2]:
        qqb = 5
    elif peaks.iloc[-1] > peaks.iloc[-2] \
            and Y.iloc[-2] > Y.iloc[-1] > bottoms.iloc[-1] > bottoms.iloc[-2] \
            and bottoms.iloc[-2] < bottoms.iloc[-3]:
        qqb = 4
    elif Y.iloc[-1] > Y.iloc[-2] \
            and Y.iloc[-1] > peaks.iloc[-1]:
        qqb = 1
    elif peaks.iloc[-1] < peaks.iloc[-2] \
            and Y.iloc[-1] < bottoms.iloc[-1]:
        qqb = -3
    elif Y.iloc[-1] < Y.iloc[-2] \
            and Y.iloc[-1] < bottoms.iloc[-1]:
        qqb = -1

    return qqb
