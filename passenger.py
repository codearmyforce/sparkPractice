'''
Generating booking datasets
Aurthor: Mr. Ravi Kumar

'''
import csv
from faker import Faker
import datetime
import random


def datagenerate(records, headers):
    fake = Faker('en_US')
    with open("passenger.csv", 'wt') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        for i in range(records):
            full_name = fake.name()
            FLname = full_name.split(" ")

            
            writer.writerow({

                    "id":random.randint(100,1000000000),
                    "Name": fake.name(),
                    "created_date":fake.date()

                    })
    
if __name__ == '__main__':
    records = 10000
    headers = ["id","Name","created_date"]
    datagenerate(records, headers)
    print("passenger CSV generation complete!")