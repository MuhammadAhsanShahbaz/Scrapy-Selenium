import os
import csv
import re
from collections import OrderedDict

import io
import fitz
import pytesseract
from PIL import Image


class PdfDataExtraction:
    def __init__(self):
        self.file_paths = self.get_input_filenames('input')
        self.output_file = 'output/output.csv'
        self.output_headers = ['Date', 'Name', 'Adress 1', 'Adress 2', 'Last Data']
        self.main()

    def main(self):
        for pdf_file in self.file_paths:
            print(f'\nGetting data of {pdf_file.replace('input\\', '')}')
            document = fitz.open(pdf_file)
            page = document.load_page(0)
            text = page.get_text()

            item = OrderedDict()
            item['Date'] = self.get_date_from_file_name(pdf_file)
            item['Name'] = pdf_file.replace('input\\', '')

            # Getting data from pdf
            if text:
                data_list = text.split('\n')
                item.update(self.get_address_data(data_list))
                item['Last Data'] = self.get_last_data(data_list)
                self.write_to_csv(item)

            else:
                # if pdf is in image form
                pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files\\Tesseract-OCR\\tesseract.exe'
                page = document.load_page(0)
                image_list = page.get_images(full=True)
                for img_index, img in enumerate(image_list):
                    xref = img[0]
                    base_image = document.extract_image(xref)
                    image_bytes = base_image["image"]
                    image = Image.open(io.BytesIO(image_bytes))
                    text = pytesseract.image_to_string(image).split('\n')

                    # Rotating the picture if required
                    if len(text) > 80:
                        image = image.rotate(180, expand=True)

                    # Perform OCR on the image
                    text = pytesseract.image_to_string(image).split('\n')

                    item['Adress 1'] = ', '.join(filter(None, [data.strip() for data in text[2:9]]))
                    item['Adress 2'] = text[18]
                    item['Last Data'] = self.get_last_data(text)
                    self.write_to_csv(item)

        print(f'\n\nRequired data from {len(self.file_paths)} file has been updated in {self.output_file.replace('output/', '')}\n')

    def get_last_data(self, data):
        text = []
        for index, d in enumerate(data[29:], start=29):
            if 'PLAINTIFF' in d:
                for tex in data[index:]:
                    if len(tex) > 1 and not ('DATE:' in tex):
                        text.append(tex)
                    else:
                        if ']' in data[index-1] or not(data[index-1]):
                            return ', '.join(text)

                        # if there is some data before 'PLAINTIFF'
                        text.insert(0, data[index - 1])
                        return ', '.join(text)

    def get_address_data(self, data_list):
        index = 0
        address = dict()

        for index, data in enumerate(data_list):
            if data == '25 ':
                index += 5
                address['Adress 1'] = ', '.join(data.strip() for data in data_list[index:index + 4])
                break

        index += 4
        for i, data in enumerate(data_list[index:index + 5]):
            if data == ' ':
                address['Adress 2'] = data_list[index + i - 1]

        if len(address) == 1:
            address['Adress 2'] = data_list[index + 4]

        return address

    def write_to_csv(self, data):
        # Storing data in the csv file
        with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.output_headers)

            # Writing headers in the file
            if csvfile.tell() == 0:
                writer.writeheader()

            # Getting data in the form of dict and writing in the csv file
            writer.writerow(data)

    def get_date_from_file_name(self, file_name):
        # Using for loop because length of the date could be 14 or less
        for i in range(14, 4, -1):
            pattern = rf'(\d{{{i}}})'
            date = re.findall(pattern, file_name)

            if len(date):
                return date[0]

        return ''

    def get_input_filenames(self, dir_path):
        file_names = []

        files = os.listdir(dir_path)

        # Getting names of all the pdf files
        for file in files:
            if file.endswith('.pdf'):
                file_path = os.path.join(dir_path, file)
                file_names.append(file_path)

        print(f'\n\n{len(file_names)} PDF files found in the directory: "{dir_path}"')

        return file_names


def main():
    pdf = PdfDataExtraction()


if __name__ == '__main__':
    main()
