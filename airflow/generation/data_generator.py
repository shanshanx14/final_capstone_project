import json, os, random, multiprocessing
from datetime import date
from faker import Faker

today = date.today()  # get today's date
path = f"../Kafka_Customers/{today}"  # creates subfolder for journaled data per day

customer = {"id": "",  # customer journaled data schema
            "active": "",
            "subscription": "",
            "customer_first_name": "",
            "customer_last_name": "",
            "cost": "",
            "start_date": "",
            "end_date": ""
            }


def get_name(number: int) -> str:  # creates a numbered json filename with latest date
    name = "customer" + str(number) + "_" + str(today) + ".json"
    return name


def get_info(journal: dict) -> dict:  # creates data for the journals
    fake = Faker()
    types = ["Basic", "Standard", "Family Package", "Premium", "Xtreme Premium",
             "Supreme", "Pay-As-Go", "Fixed Access", "Enterprise", "Supreme Plus"]
    for key in journal.keys():
        if key == "id":
            journal[key] = random.randint(1, 250)
        elif key == "active":
            journal[key] = fake.pybool()
        elif key == "subscription":
            journal[key] = random.choice(types)
        elif key == "customer_first_name":
            journal[key] = fake.first_name()
        elif key == "customer_last_name":
            journal[key] = fake.last_name()
        elif key == "cost":
            journal[key] = random.randint(100, 2000)
        elif key == "start_date":
            journal[key] = fake.date_this_decade().strftime('%Y-%m-%d')
        if key == "end_date":
            journal[key] = fake.future_date().strftime('%Y-%m-%d')
        else:
            print("Invalid keys!")
            exit(1)

    return journal


def generate():  # stores generated data as json file
    for n in range(1, 101):
        file = get_name(n)
        data = get_info(customer)
        with open(f"{path}/{file}", "w", encoding="utf-8") as outfile:
            json.dump(data, outfile, indent=4)


def start():
    if not os.path.exists(path):  # checks if path does not exist before generation begins
        os.makedirs(path)  # creates path where data will be stored
        process = multiprocessing.Process(target=generate)  # uses process pool to handle generation
        process.start()  # process starts
        process.join()  # waits for process to end

    else:  # when path exists, no data is generated and program exists
        print("Path already exists and is a directory!")
        exit(1)


if __name__ == '__main__':
    start()
