import mysql.connector
import pandas as pd
import numpy_financial as np
import csv

mydb = mysql.connector.connect(
    host="paytail-prod-read.corojbsthaze.ap-south-1.rds.amazonaws.com",
    user="bureau-service",
    password="DF7fT5kDEzVYTG",
    database="ordermanagementservice"
)

agreement_number = None
loan_tenure = None
loan_amount = None
emi_amount = None
irr_value = None
file_name = '../../Downloads/irr_values.csv'

query = '''SELECT agreement_number , loan_tenure , loan_amount , emi_amount from ordermanagementservice.nbfc_loan_detail 
where lender = "HDB" order by id desc'''

with open(file_name, mode= 'w', newline= "") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(['agreement_number', 'loan_tenure', 'loan_amount', 'emi_amount', 'irr_value'])

def calculate_irr(loan_amount, emi_amount, tenure):
    irr = list()
    for i in range(tenure + 1):
        irr.append(0)
    irr[0] = loan_amount*(-1)
    for i in range(1, tenure+1):
        irr[i] = emi_amount
    
    irr_result = np.irr(irr)
    return irr_result*1200

cursor = mydb.cursor()
cursor.execute(query)
while 1:
    rows = cursor.fetchmany(10)
    if not rows:
        break
    else:
        for result in rows:
            agreement_number = result[0]
            loan_tenure = result[1]
            loan_amount = result[2]
            emi_amount = result[3]
            irr_value = calculate_irr(loan_amount, emi_amount, loan_tenure)
            irr_value = float(f"{irr_value:.3f}")
            with open (file_name, 'a', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([agreement_number, loan_tenure, loan_amount, emi_amount, irr_value])


