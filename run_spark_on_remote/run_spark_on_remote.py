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


def copy_spark_jar_to_remote(remote_ip, local_jar_path):
    handle_path = handle_first_disk_name(local_jar_path)
    call(["scp", handle_path, "root@%s:/home/spark/" % remote_ip])


def run_shell_command_on_remote(remote_ip, local_jar_path, spark_run_command):
    ssh = paramiko.SSHClient()
    # ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(remote_ip, username="spark", password="spark")

    transport = ssh.get_transport()
    channel = transport.open_session()

    jar_name = get_jar_name(local_jar_path)
    
    # 莫名其妙的原因，命令行会在 /usr/bin/spark-submit等命令前面添加如下前缀
    final_spark_run_command = spark_run_command.replace("C:/Program Files/Git", '')

    print(final_spark_run_command)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(final_spark_run_command, get_pty=True)

    try:
        for line in iter(lambda:ssh_stdout.readline(64), ""):
            print(line, end="")

    except KeyboardInterrupt:
        # ctrl + c interrupt
        channel.close()
        ssh.close()


def get_jar_name(jar_path):
    jar_name = jar_path.split('/')[-1]
    return jar_name


def main(remote_ip, local_jar_path, spark_run_argv):
    copy_spark_jar_to_remote(remote_ip, local_jar_path)

    run_shell_command_on_remote(remote_ip, local_jar_path, ' '.join(spark_run_argv) )


if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) >= 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3:])
    else:
        print("Usage: python run_spark_on_remote.py remote_ip local_jar_path spak-run-command")
        # print("eg: python run_spark_on_remote.py ip_address com.exmind.spark.UpdateMacHomeConsumeCycle e:/IdeaProjects/SparkProject/out/artifacts/SparkProject_jar/SparkProject.jar")


    
