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
    with open("booking.csv", 'wt') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        for i in range(records):
            full_name = fake.name()
            FLname = full_name.split(" ")

            
            writer.writerow({

                    "id":fake.ean(length=13),
                    "Name": fake.name(),
                    "created_date":fake.date(),
                    "id_drive":random.randint(100,10000000000),
                    "id_passenger":random.randint(100,1000000000),
                    "rating":random.randint(1,10),
                    "start_date":fake.date(),
                    "end_date":fake.date(),
                    "point":random.randint(1,1000000000)

                    })
    
if __name__ == '__main__':
    records = 10000
    headers = ["id","Name","created_date","id_drive","id_passenger","rating","start_date","end_date","point"]
    datagenerate(records, headers)
    print("booking CSV generation complete!")