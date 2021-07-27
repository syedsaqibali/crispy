#!/usr/bin/env python

import argparse             # To parse command-line arguments
from tqdm import tqdm       # To draw a nifty progress-bar. From here: https://github.com/tqdm/tqdm
import csv                  # To read & write CSV files
import yaml                 # To parse YAML configuration file
import re                   # To use regular expressions
import subprocess           # To make a system call
import datetime             # To manipulate dates. Not used in the code but required by the configuration file (that's a hack)
import sys                  # To abort the program early

# Define the valid data types used by the configuration file
DATA_TYPES = {
    'INTEGER',
    'FLOAT',
    'STRING'
}

# Define what a variable will look like in the configuration file
VARIABLE_REGEXP = re.compile('\$\{[A-Za-z0-9 ]+\}')


# Define the interface for Inputters
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

# Define the interface for Outputters
class OutputInterface:
    def __init__(self, target, transforms):
        raise NotImplementedError
    def put_row(self, data):
        raise NotImplementedError
    def tear_down(self):
        raise NotImplementedError

# Define a class that implements the InputInterface for reading from a CSV file
class CSVFileInput(InputInterface):
    def __init__(self, source, transforms):
        self.source = source
        self.transforms = transforms
        self.current_row_num = 0
        self.total_rows = self._num_rows_in_file()
        self.open_file = open(self.source, newline='')
        self.reader = csv.DictReader(self.open_file)

    # The iterator functionality that gets called when an instance of this class
    # is looped over. It reads a row of data from the input file, transforms
    # the fields to the specified type and returns the data. Any malformed
    # data that cannot be transformed causes an exception to be thrown
    def __next__(self):
        self.current_row_num += 1
        file_row = self.reader.__next__()
        transformed_row = {}
        for field in file_row:
            type = self.transforms[field]['Type']
            try:
                if type == 'INTEGER':
                    transformed_row[field] = int(file_row[field])
                elif type == 'FLOAT':
                    transformed_row[field] = float(file_row[field])
                elif type == 'STRING':
                    transformed_row[field] = file_row[field]
                else:
                    assert False, 'It should never get here!'
            except ValueError as e:
                raise ValueError('Casting of input field {} failed: {}'.format(field, str(e))) from e
        return transformed_row

    # Report the total number of rows of data in this file
    def num_total_rows(self):
        return self.total_rows

    # Report the current row number
    def get_current_row_num(self):
        return self.current_row_num

    # Clean Up
    def tear_down(self):
        self.open_file.close()

    # Calculate the total number of rows of data in this file
    def _num_rows_in_file(self):
        p = subprocess.Popen(['wc', '-l', self.source],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        result, err = p.communicate()
        if p.returncode != 0:
            raise IOError(err)
        return int(result.strip().split()[0])


# Define a class that implements the OutputInterface for writing to a CSV file
class CSVFileOutput(OutputInterface):
    def __init__(self, target, transforms):
        self.target = target
        self.transforms = transforms
        self.open_file = open(self.target, 'w')
        self.writer = csv.DictWriter(self.open_file, self.transforms.keys())
        self.writer.writeheader()

    # Substitue the (possibly multiple occurrences) of variables in the
    # transform with their actual values from data
    def _substitute_variables(self, transform_string, data):
        replacement_set = set(VARIABLE_REGEXP.findall(transform_string))
        for i in replacement_set:
            # Remove the leading ${ and the trailing } so we have
            # the substitution variable name's
            source_field_name = i[2:-1]
            transform_string = transform_string.replace(i, str(data[source_field_name]))
        return transform_string

    # Take a record, transform it according to the configuration and output it
    def put_row(self, data):
        transformed_row = {}
        for field in self.transforms.keys():
            try:
                transformed_value = eval(
                    self._substitute_variables(transform_string=self.transforms[field]['Transform'],
                                               data=data))
            except Exception as e:
                raise Exception('Transforming/substitution of output field {} failed: {}'.format(field, str(e))) from Exception
            type = self.transforms[field]['Type']
            try:
                if type == 'INTEGER':
                    transformed_row[field] = int(transformed_value)
                elif type == 'FLOAT':
                    transformed_row[field] = float(transformed_value)
                elif type == 'STRING':
                    transformed_row[field] = transformed_value
                else:
                    assert False, 'It should never get here!'
            except ValueError as e:
                raise ValueError('Casting for output field {} failed: {}'.format(field, str(e))) from e
        self.writer.writerow(transformed_row)

    # Clean up
    def tear_down(self):
        self.open_file.close()

# Parse and validate the configuration file. If it is malformed throw an exception
def parse_config_yaml(config_file):

    # Parse the configuration file
    parsed_config = yaml.load(open(config_file).read(), Loader=yaml.FullLoader)

    # Check the configuration has top-level keys input-fields & output-fields only
    assert set(parsed_config.keys()) == {'input-fields', 'output-fields'}, 'There must be exactly 2 top-level keys: input-fields, output-fields'

    # Check that all input fields have a valid Type
    assert set([parsed_config['input-fields'][i].get('Type') for i in parsed_config['input-fields']]).issubset(DATA_TYPES), 'All input fields must have a valid Type'

    # Check that all output fields have a valid Type
    assert set([parsed_config['output-fields'][i].get('Type') for i in parsed_config['output-fields']]).issubset(DATA_TYPES), 'All output fields must have a valid Type'

    # Check that all output fields have a Transform
    assert all(['Transform' in parsed_config['output-fields'][i] for i in parsed_config['output-fields']]), 'All output fields must have a Transform'

    return parsed_config

# The Main function
def main():
    # Parse the command-line arguments
    parser = argparse.ArgumentParser()
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-c', '--config', required=True, help='Configuration specification file')
    requiredNamed.add_argument('-i', '--input', required=True, help='Input CSV file')
    requiredNamed.add_argument('-s', '--success', required=True, help='Output success file')
    requiredNamed.add_argument('-f', '--failure', required=True, help='Output failure file')
    args = parser.parse_args()

    # Get the configuration
    try:
        config_spec = parse_config_yaml(config_file=args.config)
    except (yaml.YAMLError, AssertionError) as e:
        print("Failed to parse configuration file {}: {}".format(args.config, str(e)))
        sys.exit(-1)

    # Go through the Input CSVfile, transforming the data according to the configuration file and
    # and output the transformed row to a CSV file
    inputter = CSVFileInput(source=args.input, transforms=config_spec['input-fields'])
    outputter = CSVFileOutput(target=args.success, transforms=config_spec['output-fields'])

    reject_file = open(args.failure, 'w')
    reject_writer = csv.DictWriter(reject_file, ['Input Row Number', 'Error Description'])
    reject_writer.writeheader()

    success_count = failure_count = 0
    for _ in tqdm(iter(range(1, inputter.num_total_rows()+1)), total=inputter.num_total_rows()):
        try:
            row = inputter.__next__()
            outputter.put_row(row)
            success_count += 1
        except Exception as e:
            failure_count += 1
            reject_writer.writerow({
                'Input Row Number': inputter.current_row_num,
                'Error Description': str(e)
            })
    outputter.tear_down()
    inputter.tear_down()
    print('{} rows successfully transformed in file {}'.format(success_count, args.success))
    print('{} rows failed to transform in file {}'.format(failure_count, args.failure))

if __name__ == '__main__':
    main()
