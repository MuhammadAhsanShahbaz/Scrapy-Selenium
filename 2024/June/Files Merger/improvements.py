import os
import csv


class Merger:
    def __init__(self):
        self.output_file = 'output/test2.csv'

        self.input = self.get_input_filenames('input')
        self.master = self.get_input_filenames('master')

        self.master_apn_info = self.get_data_from_file(self.master, 'master')
        self.input_info = self.get_data_from_file(self.input, 'input')

    def get_data_from_file(self, files_list, file_type):
        for file in files_list:
            data = []

            if file.endswith('.csv'):
                csv_data = self.read_csv(file)
                data += csv_data

                if file_type == 'input':
                    self.remove_duplication(data, file)

        if file_type == 'master':
            return [row.get('APN', '') for row in data if 'APN' in row]

        return

    def read_csv(self, filename):
        data = []

        print(f'\n\nGetting data from CSV file: "{filename}"')

        with open(filename, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                data.append(row)

        print(f'{len(data)} Rows fetched')
        return data

    def get_input_filenames(self, dir_path):
        file_names = []

        files = os.listdir(dir_path)

        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(dir_path, file)
                file_names.append(file_path)

        print(f'\n\n{len(file_names)} CSV files found in the directory: "{dir_path}"')

        return file_names

    def write_to_csv(self, data, output_file):
        if data:
            output_headers = data[0].keys()

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=output_headers)

                if csvfile.tell() == 0:
                    writer.writeheader()

                for row in data:
                    writer.writerow(row)

        else:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                pass

    def remove_duplication(self, data, file_name):
        filtered_data = []
        duplication_count = 0

        for row in data:
            if row.get('APN', '') not in self.master_apn_info:
                filtered_data.append(row)

            else:
                duplication_count = duplication_count + 1

        print(f'\n\n{duplication_count} out of {len(data)} rows is/are found and removed from the {file_name}')

        self.write_to_csv(filtered_data, file_name)


def main():
    merger = Merger()


if __name__ == "__main__":
    main()
