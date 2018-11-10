# 入口
import sys
import getopt
from _datetime import date


def usage():
    print("-f: batch文件名\n-n: batch文件类名\n-h：提供版本")


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
            from sqlalchemy import create_engine
            from conf.myapp import db_config
            from globalvar import GL

            db_engine = create_engine(
                'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'.format(**db_config), echo=True)
            # db_engine = create_engine(
            #     'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'.format(**db_config), echo=True)
            GL.set_value('db_engine', db_engine)

            if start_date or end_date:
                executor.execute(start_date, end_date)
            else:
                executor.execute()
        else:
            print('此文件没有execute函数')
    except BaseException as e:
        print(e)

