#!/usr/bin/env python

import argparse
from tqdm import tqdm
import csv
import yaml
import re
import subprocess
import datetime

DATA_TYPES = {
    'INTEGER',
    'FLOAT',
    'STRING'
}

VARIABLE_REGEXP = re.compile('\$\{[A-Za-z0-9 ]+\}')

class InputInterface:
    def __init__(self, source, transforms):
        raise NotImplementedError
    def __next__(self):
        raise NotImplementedError
    def tear_down(self):
        raise NotImplementedError
    def num_total_rows(self):
        raise NotImplementedError
    def get_current_row_num(self):
        raise NotImplementedError
    def __iter__(self):
        return self


class OutputInterface:
    def __init__(self, target, transforms):
        raise NotImplementedError
    def put_row(self, data):
        raise NotImplementedError
    def get_current_row_num(self):
        raise NotImplementedError
    def tear_down(self):
        raise NotImplementedError


class FileInput(InputInterface):
    def __init__(self, source, transforms):
        self.source = source
        self.transforms = transforms
        self.current_row_num = 0
        self.total_rows = self._num_rows_in_file()
        self.open_file = open(self.source, newline='')
        self.reader = csv.DictReader(self.open_file)

    def __next__(self):
        file_row = self.reader.__next__()
        self.current_row_num += 1
        transformed_row = {}
        for field in file_row:
            type = self.transforms[field]['Type']
            if type == 'INTEGER':
                transformed_row[field] = int(file_row[field])
            elif type == 'FLOAT':
                transformed_row[field] = float(file_row[field])
            elif type == 'STRING':
                transformed_row[field] = file_row[field]
            else:
                assert False, 'It should never get here!'
        return transformed_row

    def num_total_rows(self):
        return self.total_rows

    def get_current_row_num(self):
        return self.current_row_num

    def tear_down(self):
        self.open_file.close()

    def _num_rows_in_file(self):
        p = subprocess.Popen(['wc', '-l', self.source],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        result, err = p.communicate()
        if p.returncode != 0:
            raise IOError(err)
        return int(result.strip().split()[0])


class FileOutput(OutputInterface):
    def __init__(self, target, transforms):
        self.target = target
        self.transforms = transforms
        self.current_row_num = 0
        self.open_file = open(self.target, 'w')
        self.writer = csv.DictWriter(self.open_file, self.transforms.keys())
        self.writer.writeheader()

    def _substitute_variables(self, transform_string, data):
        replacement_set = set(VARIABLE_REGEXP.findall(transform_string))
        for i in replacement_set:
            source_field_name = i[2:-1]
            transform_string = transform_string.replace(i, str(data[source_field_name]))
        return transform_string

    def put_row(self, data):
        transformed_row = {}
        self.current_row_num += 1
        for field in self.transforms.keys():
            transformed_value = eval(self._substitute_variables(transform_string=self.transforms[field]['Transform'],
                                                                data=data))
            type = self.transforms[field]['Type']
            if type == 'INTEGER':
                transformed_row[field] = int(transformed_value)
            elif type == 'FLOAT':
                transformed_row[field] = float(transformed_value)
            elif type == 'STRING':
                transformed_row[field] = transformed_value
            else:
                assert False, 'It should never get here!'
        self.writer.writerow(transformed_row)

    def get_current_row_num(self):
        return self.current_row_num

    def tear_down(self):
        self.open_file.close()

def parse_config_yaml(config_file):

    # Parse the configuration file
    parsed_config = yaml.load(open(config_file).read(), Loader=yaml.FullLoader)

    # Check the configuration has top-level keys input-fields & output-fields only
    assert set(parsed_config.keys()) == {'input-fields', 'output-fields'}, 'Configuration file {} must contain exactly 2 top-level keys: input-fields, output-fields'.format(config_file)

    # Check that all input fields have a valid Type
    assert set([parsed_config['input-fields'][i].get('Type') for i in parsed_config['input-fields']]).issubset(DATA_TYPES), 'All input fields in configuration file {} must have a valid Type'.format(config_file)

    # Check that all output fields have a valid Type
    assert set([parsed_config['output-fields'][i].get('Type') for i in parsed_config['output-fields']]).issubset(DATA_TYPES), 'All output fields in configuration file {} must have a valid Type'.format(config_file)

    # Check that all output fields have a Transform
    assert all(['Transform' in parsed_config['output-fields'][i] for i in parsed_config['output-fields']]), 'All output fields in configuration file {} must have a Transform'.format(config_file)

    return parsed_config


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

    inputter = FileInput(source=args.input, transforms=config_spec['input-fields'])
    outputter = FileOutput(target=args.success, transforms=config_spec['output-fields'])
    for row in tqdm(inputter, total=inputter.num_total_rows()):
        outputter.put_row(row)
    outputter.tear_down()
    inputter.tear_down()

if __name__ == '__main__':
    main()
