
配置文件命令
cp conf/myapp_example.py conf/myapp.py


# batch命令

## 项目初始化命令
*** python index.py -n fetch_data --sd='' --ed='20000101'
*** python index.py -n fetch_data --sd='20000101' --ed='20100101'
*** python index.py -n fetch_data --sd='20100101' --ed=''
*** python index.py -n update_basic_tables  --sd='' --ed='20180101'
*** python index.py -n update_basic_tables  --sd='20180101' --ed=''
    初始化表，初始化基本日线行情信息


## 每日一次
*** today=$(date +%Y%m%d) && python index.py -n fetch_data --sd=$today --ed=$today
    更新日线行情基本信息

*** python index.py -n update_basic_tables --sd=$today --ed=$today
    更新stock_basic表，每日更新一次上市的股票基本信息