#!/usr/bin/env python

import argparse
from tqdm import tqdm
import csv
import ruamel.yaml as yaml
import sqlite3
import re
from datetime import datetime
import subprocess

field_regexp = re.compile('\$\{[A-Za-z0-9 ]+\}')

def lines_in_file(fname):
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return int(result.strip().split()[0])

def parse_config_yaml(config_file):
    with open(config_file) as stream:
        return yaml.safe_load(stream)

def init_db(db_file, config_spec):
    con = sqlite3.connect(db_file)
    cur = con.cursor()

    type_map = {
        'INTEGER': 'integer',
        'STRING': 'text',
        'FLOAT': 'real',
        'DATE': 'text',
    }

    sql_query = '\n'.join([
        'CREATE TABLE products',
        '(',
        '    {}'.format(',\n    '.join(['{} {}'.format(i, type_map[config_spec['output-fields'][i]['Type']]) for i in config_spec['output-fields']])),
        ')'
    ])
    print('sql_query = {}'.format(sql_query))
    cur.execute(sql_query)
    con.commit()
    return con


def evaluate_transform(transform, type, source_dict):
    replacement_set = set(field_regexp.findall(transform))
    for i in replacement_set:
        transform = transform.replace(i, source_dict[i[2:-1]])
    return eval(transform)

def prepare_value_for_db_insert(value, type):
    if type == 'INTEGER':
        return str(value)
    if type == 'STRING':
        return '\'{}\''.format(value)
    if type == 'FLOAT':
        return str(value)
    if type == 'DATE':
        return '\'{}\''.format(value.strftime("%Y-%m-%d"))

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
        db_conn = init_db(db_file=args.database,
                          config_spec=config_spec)
    except sqlite3.Error as exc:
        print(exc)

    db_cursor = db_conn.cursor()
    total_lines = lines_in_file(args.input) - 1
    with open(args.input, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        for row_ind, row in tqdm(enumerate(reader), total=total_lines):
            print('*' * 80)
            sql_query = '\n'.join([
                'INSERT INTO products',
                '(',
                '    {}'.format(',\n    '.join([i for i in config_spec['output-fields']])),
                ') VALUES (',
                '    {}'.format(',\n    '.join([prepare_value_for_db_insert(value=evaluate_transform(transform=config_spec['output-fields'][i]['Transform'],
                                                                                                     type=config_spec['output-fields'][i]['Type'],
                                                                                                     source_dict=row),
                                                                            type=config_spec['output-fields'][i]['Type']) for i in config_spec['output-fields']])),
                ')'
            ])
            print(sql_query)
            db_cursor.execute(sql_query)
            db_conn.commit()
    db_conn.close()

if __name__ == '__main__':
    main()
