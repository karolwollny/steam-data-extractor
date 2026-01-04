# stop if any process is running
/opt/homebrew/Cellar/apache-spark/4.1.0/libexec/sbin/stop-master.sh
/opt/homebrew/Cellar/apache-spark/4.1.0/libexec/sbin/stop-worker.sh
/opt/homebrew/Cellar/apache-spark/4.1.0/libexec/sbin/stop-history-server.sh

# start new processes
/opt/homebrew/Cellar/apache-spark/4.1.0/libexec/sbin/start-master.sh --host localhost --port 7077
/opt/homebrew/Cellar/apache-spark/4.1.0/libexec/sbin/start-worker.sh spark://localhost:7077 -c 2 -m 2g
/opt/homebrew/Cellar/apache-spark/4.1.0/libexec/sbin/start-history-server.sh