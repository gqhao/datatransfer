#!/bin/sh

cnt=$(ps aux |grep datatransfer_from_mssql_to_mysql.py |grep -v "grep"|wc -l)

if [ $cnt -eq 0 ]
then
    sleep 5
	cnt=$(ps aux |grep datatransfer_from_mssql_to_mysql.py |grep -v "grep"|wc -l)	
	if [ $cnt -eq 0 ]
	then
		echo "datatransfer_from_mssql_to_mysql.py don't run, I will start it"
		nohup python /usr/local/git_workspace/distributed_datatransfer_archtecture/source_code/datatransfer_from_mssql_to_mysql.py &
		exit
	fi
fi
