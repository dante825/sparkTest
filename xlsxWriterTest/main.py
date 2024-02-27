import xlsxwriter
import pandas as pd

input_file = "/Users/kangwei/development/workspaces/pyWorkspace/sparkTest/xlsxWriterTest/input_data.csv"
output_file = "/Users/kangwei/development/workspaces/pyWorkspace/sparkTest/xlsxWriterTest/output.xlsx"

df = pd.read_csv(input_file, header=0)

# print(df.head())

workbook = xlsxwriter.Workbook(output_file)
worksheet = workbook.add_worksheet()
worksheet.write('A1', 'Hello world')
worksheet.protect('123456')
workbook.close()