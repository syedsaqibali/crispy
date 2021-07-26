#!/usr/bin/env python

import argparse
from tqdm import tqdm
import csv
import ruamel.yaml as yaml
import sqlite3
import re
from datetime import datetime
import subprocess
import sys

DATA_TYPES = {
    'INTEGER',
    'FLOAT',
    'STRING'
}


class InputInterface:
    def __init__(self, source, transforms):
        raise NotImplementedError
    def get_next_row(self):
        raise NotImplementedError
    def tear_down(self):
        raise NotImplementedError
    def num_total_rows(self):
        raise NotImplementedError
    def get_current_row(self):
        raise NotImplementedError

class OutputInterface:
    def __init__(self, target, transforms):
        raise NotImplementedError
    def put_row(self, data):
        raise NotImplementedError
    def get_current_row(self):
        raise NotImplementedError
    def tear_down(self):
        raise NotImplementedError


class FileInput(InputInterface):
    def __init__(self, source, transforms):
        self.source = source
        self.transforms = transforms
        self.current_row_num = 0
    def get_next_row(self):
        pass
    def num_total_rows(self):
        pass
    def get_current_row(self):
        pass
    def tear_down(self):
        pass


class FileOutput(OutputInterface):
    def __init__(self, target, transforms):
        self.target = target
        self.transforms = transforms
        self.current_row_num = 0
    def put_row(self, data):
        pass
    def get_current_row(self):
        pass
    def tear_down(self):
        pass



field_regexp = re.compile('\$\{[A-Za-z0-9 ]+\}')




def lines_in_file(fname):
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return int(result.strip().split()[0])


def parse_config_yaml(config_file):

    # Parse the configuration file
    with open(config_file) as stream:
        parsed_config = yaml.safe_load(stream)

    # Check the configuration has top-level keys input-fields & output-fields only
    assert set(parsed_config.keys()) == {'input-fields', 'output-fields'}, 'Configuration file {} must contain exactly 2 top-level keys: input-fields, output-fields'.format(config_file)

    # Check that all input fields have a valid Type
    assert set([parsed_config['input-fields'][i].get('Type') for i in parsed_config['input-fields']]).issubset(DATA_TYPES), 'All input fields in configuration file {} must have a valid Type'.format(config_file)

    # Check that all output fields have a valid Type
    assert set([parsed_config['output-fields'][i].get('Type') for i in parsed_config['output-fields']]).issubset(DATA_TYPES), 'All output fields in configuration file {} must have a valid Type'.format(config_file)

    # Check that all output fields have a Transform
    assert all(['Transform' in parsed_config['output-fields'][i] for i in parsed_config['output-fields']]), 'All output fields in configuration file {} must have a Transform'.format(config_file)

    return parsed_config





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
    requiredNamed.add_argument('-i', '--input',
                               required=True,
                               help='Input CSV file')
    requiredNamed.add_argument('-s', '--success',
                               required=True,
                               help='Output success file')
    requiredNamed.add_argument('-f', '--failure',
                               required=True,
                               help='Output failure file')
    args = parser.parse_args()

    config_spec = parse_config_yaml(config_file=args.config)
    print('config_spec = {}'.format(config_spec))

    inputter = FileInput(source=args.input, transforms=config_spec['input-fields'])
    outputter = FileOutput(target=args.success, transforms=config_spec['output-fields'])
    print('Hello World')
    outputter.tear_down()
    inputter.tear_down()

    # try:
    #     db_conn = init_db(db_file=args.database,
    #                       config_spec=config_spec)
    # except sqlite3.Error as exc:
    #     print(exc)
    #
    # db_cursor = db_conn.cursor()
    # total_lines = lines_in_file(args.input) - 1
    # with open(args.input, newline='') as csv_file:
    #     reader = csv.DictReader(csv_file)
    #     for row_ind, row in tqdm(enumerate(reader), total=total_lines):
    #         print('*' * 80)
    #         sql_query = '\n'.join([
    #             'INSERT INTO products',
    #             '(',
    #             '    {}'.format(',\n    '.join([i for i in config_spec['output-fields']])),
    #             ') VALUES (',
    #             '    {}'.format(',\n    '.join([prepare_value_for_db_insert(value=evaluate_transform(transform=config_spec['output-fields'][i]['Transform'],
    #                                                                                                  type=config_spec['output-fields'][i]['Type'],
    #                                                                                                  source_dict=row),
    #                                                                         type=config_spec['output-fields'][i]['Type']) for i in config_spec['output-fields']])),
    #             ')'
    #         ])
    #         print(sql_query)
    #         db_cursor.execute(sql_query)
    #         db_conn.commit()
    # db_conn.close()

if __name__ == '__main__':
    main()
