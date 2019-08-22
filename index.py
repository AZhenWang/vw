#coding: utf-8
# 入口
import sys
import getopt
from datetime import datetime, timedelta
from conf.myapp import init_date


def usage():
    print("-n: batch文件类名\n-h：提供版本")


file_name = ""
start_date = ''
end_date = ''

opts, _ = getopt.getopt(sys.argv[1:], "hn:", ['sd=', 'ed='])
for op, value in opts:
    if op == "-n":
        file_name = value

    elif op == "-h":
        usage()
        sys.exit()

    elif op == "--ed":
        end_date = value

    elif op == "--sd":
        start_date = value

if file_name:

    try:
        module_name = 'app.batches.{}'.format(file_name)
        executor = __import__(module_name, fromlist=('batches', file_name))
        if hasattr(executor, "execute"):

            now = datetime.now()
            today = now.strftime('%Y%m%d')
            if not start_date:
                start_date = init_date
            if not end_date or end_date >= today:
                yesterday = (now - timedelta(1)).strftime('%Y%m%d')
                hour = now.hour
                if hour < 15:
                    if start_date == end_date:
                        start_date = yesterday
                    end_date = yesterday
                else:
                    if start_date == end_date:
                        start_date = today
                    end_date = today
            executor.execute(start_date, end_date)
        else:
            print('此文件没有execute函数')
    except BaseException as e:
        print(e)

