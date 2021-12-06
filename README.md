# prometheus-python-exporter

需要aws的CloudWatchReadOnlyAccess Policy權限


shell執行方式:
$ aws configure
AWS Access Key ID [********************]: enter_your_access_key_here
AWS Secret Access Key [********************]: enter_your_secret_key_here
Default region name [eu-west-1]: 
Default output format [None]:
$ python exporter_hook.py



Docker方式:
docker build -t prometheus-python-exporter .
docker run -d -p 9120:9120 prometheus-python-exporter


測試:
curl -i 127.0.0.1:9120/rds/metrics
