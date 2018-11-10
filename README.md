# batch命令

## 项目初始化命令
*** python index.py -n fetch_data --sd='' --ed=''
    初始化表，初始化基本日线行情信息


## 每日一次
*** today=$(date +%Y%m%d) && python index.py -n fetch_data --sd=$today --ed=$today
    更新日线行情基本信息



## 每月一次
*** python index.py -n update_stock_basic
    更新stock_basic表，每月新增一次新上市的股票基本信息