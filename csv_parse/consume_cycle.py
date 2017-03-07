# coding:utf-8

import csv

"""
create table base_consume_cycle_coordinates(
    name    string,
    center_lng  string,
    center_lat  string,
    sort_id int,
    lng string,
    lat string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

insert overwrite table base_consume_cycle_coordinates select * from base_consume_cycle_coordinates where 1 >2;

load data local inpath '/home/hdfs/output.csv' overwrite into table  masa_td.base_consume_cycle_coordinates;

select * from base_consume_cycle_coordinates limit 10;
"""


def read_csv_file():
    with open("./consume_cycle.csv") as csvfile:

        output_file = open('output.csv', 'wb')
        output_writer = csv.writer(output_file)

        reader = csv.reader(csvfile)
        count = -1

        for row in reader:
            count += 1
            if count == 0:
                continue
            else:
                last_col_pair_cont = 1
                last_col_list = handle_last_col(row[3])

                for long_lati_pair in last_col_list:
                    output_writer.writerow([row[0], row[1], row[2], last_col_pair_cont, long_lati_pair[0], long_lati_pair[1]])
                    last_col_pair_cont += 1
            # else:
            #     break

        output_file.close()


 
def handle_last_col(last_col):
    first_step = last_col.lstrip('MULTIPOLYGON(((').rstrip(')))')
    long_lati_list = first_step.split(',')

    final = []

    for pair in long_lati_list:
        final.append( pair.split(' ') )

    return final




read_csv_file()