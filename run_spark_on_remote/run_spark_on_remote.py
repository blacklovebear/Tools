#coding:utf-8
#python
from __future__ import print_function
from subprocess import call
import paramiko
import sys

def handle_first_disk_name(input_path):
    path_list = input_path.split("/")
    path_list[0] = '/%s' % (path_list[0][0:1].lower())
    return '/'.join(path_list)


def copy_spark_jar_to_remote(local_spark_jar_path, remote_ip):
    handle_path = handle_first_disk_name(local_spark_jar_path)
    call(["scp", handle_path, "root@%s:/home/spark/" % remote_ip])


def run_shell_command_on_remote(remote_ip, local_spark_jar_path, spark_main_class):
    ssh = paramiko.SSHClient()
    # ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(remote_ip, username="spark", password="spark")

    jar_name = get_jar_name(local_spark_jar_path)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(
        "/usr/bin/spark-submit --master yarn-client --class %s /home/spark/%s" % (spark_main_class, jar_name), get_pty=True)

    for line in iter(lambda:ssh_stdout.readline(64), ""):
        print(line, end="")


def get_jar_name(jar_path):
    jar_name = jar_path.split('/')[-1]
    return jar_name


def main(spark_main_class, local_spark_jar_path):
    remote_ip = "172.30.103.11"
    copy_spark_jar_to_remote(local_spark_jar_path, remote_ip)
    run_shell_command_on_remote(remote_ip, local_spark_jar_path, spark_main_class)


if __name__ == "__main__":
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python run_spark_on_remote.py spark_class local_jar_path")
        print("eg: python run_spark_on_remote.py com.exmind.spark.UpdateMacHomeConsumeCycle e:/IdeaProjects/SparkProject/out/artifacts/SparkProject_jar/SparkProject.jar")


    
