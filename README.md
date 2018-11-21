
配置文件命令
cp conf/myapp_example.py conf/myapp.py


# batch命令

## 项目初始化命令

*** python index.py -n fetch_data --sd='' --ed='20000101'
*** python index.py -n fetch_data --sd='20000101' --ed='20100101'
*** python index.py -n fetch_data --sd='20100101' --ed=''
    初始化表，初始化基本日线行情信息

*** python index.py -n init_features
    初始化特征表和特征组合表

*** python index.py -n update_threshold --sd='' --ed='20000101'
*** python index.py -n update_threshold --sd='20000101' --ed='20100101'
*** python index.py -n update_threshold --sd='20100101' --ed=''
    初始化thresholds表


## 每日一次
*** today=$(date +%Y%m%d) && python index.py -n fetch_data --sd=$today --ed=$today
    更新日线行情基本信息

*** today=$(date +%Y%m%d) && python index.py -n run_knn_model --sd=$today --ed=$today
    每日跑一次knn模型, 每当执行一次init_features命令之后，就需要truncate classified_v表，再执行这个run_knn_model操作