#import modules
import xml.etree.ElementTree as ET
import mysql.connector
from xmljson import badgerfish as bf
from xml.etree.ElementTree import fromstring
import json
import boto3

#create connection to mysql database
mydb = mysql.connector.connect(
  host="host",
  user="user",
  passwd="password",
  database="database"
)

mycursor = mydb.cursor()

#Database setup queries
'''mycursor.execute("CREATE DATABASE employeeDatabase")
mycursor.execute("CREATE TABLE employees (id INT AUTO_INCREMENT PRIMARY KEY, firstname VARCHAR(255), lastname VARCHAR(255), contact VARCHAR(10))")
mycursor.execute("CREATE TABLE addresses (id INT AUTO_INCREMENT PRIMARY KEY, housenumber VARCHAR(255), street VARCHAR(255), permanent VARCHAR(1), pincode VARCHAR(6), employeeId INT, CONSTRAINT fk_employeeid FOREIGN KEY (employeeId) REFERENCES employees(id))")
sql = "INSERT INTO employees (firstname, lastname, contact) VALUES (%s, %s, %s)"
val = ("Prateek", "Sethi", "9876644211")
mycursor.execute(sql, val)
sql = "INSERT INTO addresses (housenumber, street, permanent, pincode, employeeId) VALUES (%s, %s, %s, %s, %s)"
val = ("24", "12", "Y", "122001", 2)
mycursor.execute(sql, val)
sql = "INSERT INTO addresses (housenumber, street, permanent, pincode, employeeId) VALUES (%s, %s, %s, %s, %s)"
val = ("25", "10", "N", "122022", 2)
mycursor.execute(sql, val)
mydb.commit()'''

#Creating xml from mysql tables data
mycursor.execute("SELECT * FROM employees")
exployeesresult = mycursor.fetchall()
data = ET.Element('EmployeeData')
for x in exployeesresult:
    employee = ET.SubElement(data, 'employee')
    ID = ET.SubElement(employee, 'ID')
    ID.text = str(x[0])
    firstname = ET.SubElement(employee, 'firstname')
    firstname.text = x[1]
    lastname = ET.SubElement(employee, 'lastname')
    lastname.text = x[2]
    contact = ET.SubElement(employee, 'contact')
    contact.text = x[3]
    query_str = "SELECT * FROM addresses as a where a.employeeId = " + str(x[0])
    mycursor.execute(query_str)
    addressesresult = mycursor.fetchall()
    address_data = ET.SubElement(employee, 'addresses')
    for y in addressesresult:
        address = ET.SubElement(address_data, 'address')
        housenumber = ET.SubElement(address, 'housenumber')
        housenumber.text = y[1]
        street = ET.SubElement(address, 'street')
        street.text = y[2]
        pincode = ET.SubElement(address, 'pincode')
        pincode.text = y[4]





#creating json out of xml
tree_root = fromstring(ET.tostring(data))
employee_data_json = json.dumps(bf.data(tree_root))
employee_data_json = json.loads(employee_data_json)

#Writing employee data into dynamodb
dynamodb = boto3.resource('dynamodb', region_name='aws-region', endpoint_url="dynamodb-url" , aws_access_key_id='id',
         aws_secret_access_key='key')

table = dynamodb.Table('table-name')
for employee in employee_data_json.get('table-name').get('employee'):
      employee['ID'] = employee['ID']['$']
      table.put_item(
          Item=employee)

print("Data successfully loaded into dynamodb..")


