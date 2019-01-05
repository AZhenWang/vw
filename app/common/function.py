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
            buy_price = prices[i]
            cum_return -= fee_rate

        if holdings[i - 1] == 0 and holdings[i - 2] != 0 and buy_price != 0:
            cum_return -= fee_rate * prices[i] / buy_price

        if holdings[i - 1] == 1:
            sell_price = prices[i]
            diff = sell_price - buy_price
            return_from_entry = diff / min([sell_price, buy_price])

        cum_return_set.append(cum_return + return_from_entry)

        if holdings[i - 1] == 0 and holdings[i - 2] == 1:
            cum_return += return_from_entry
            return_from_entry = 0

    return cum_return_set


def get_buy_sell_points(holdings):
    import numpy as np

    buy, sell = [], []
    for i in range(len(holdings)):
        if i >= 1 and holdings[i] != holdings[i - 1]:
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

