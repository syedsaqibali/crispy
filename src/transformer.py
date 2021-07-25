#!/usr/bin/env python

import argparse
import tqdm
import csv
import ruamel.yaml as yaml
import sqlite3

print("Hello World!")




def parse_config_yaml(config_file):
    with open(config_file) as stream:
        return yaml.safe_load(stream)

def setup_db(db_file, config_spec):
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    sql_query = '\n'.join([
        'CREATE TABLE products',
        '(',
        '    {}'.format(',\n    '.join(['{} {}'.format(i, config_spec['output-fields'][i]['Type']) for i in config_spec['output-fields']])),
        ')'
    ])
    print('sql_query = {}'.format(sql_query))
    cur.execute(sql_query)

def evaluate_transform(transform, type, source_dict):
    return transform


def main():
    parser = argparse.ArgumentParser()

    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-c', '--config',
                               required=True,
                               help='Configuration specification file')
    requiredNamed.add_argument('-d', '--database',
                               required=True,
                               help='SQLLite database file')
    requiredNamed.add_argument('-i', '--input',
                               required=True,
                               help='Input CSV file')
    requiredNamed.add_argument('-f', '--failure',
                               required=True,
                               help='Output failure report file')
    args = parser.parse_args()

    try:
        config_spec = parse_config_yaml(config_file=args.config)
    except yaml.YAMLError as exc:
        print(exc)
    print('config_spec = {}'.format(config_spec))

    try:
        db_conn = setup_db(db_file=args.database,
                       config_spec=config_spec)
    except sqlite3.Error as exc:
        print(exc)


    with open(args.input, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            print('*' * 80)
            print('\n'.join(['{}: {}'.format(i,
                                             evaluate_transform(transform=config_spec['output-fields'][i]['Transform'],
                                                                type=config_spec['output-fields'][i]['Type'],
                                                                source_dict=row)) for i in config_spec['output-fields']]))



if __name__ == '__main__':
    main()
