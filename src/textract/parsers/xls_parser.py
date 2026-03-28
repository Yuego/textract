import xlrd

from .utils import BaseParser


class Parser(BaseParser):
    """Extract text from Excel files (.xls).
    """

    def extract(self, filename, **kwargs):
        workbook = xlrd.open_workbook(filename)
        output = "\n"
        for name in workbook.sheet_names():
            worksheet = workbook.sheet_by_name(name)
            for curr_row in range(worksheet.nrows):
                new_output = []
                for index_col in range(worksheet.ncols):
                    value = worksheet.cell_value(curr_row, index_col)
                    if value:
                        if isinstance(value, (int, float)):
                            value = str(value)
                        new_output.append(value)
                if new_output:
                    output += ' '.join(new_output) + '\n'
        return output
