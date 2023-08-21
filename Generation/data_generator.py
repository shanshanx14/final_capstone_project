import json, os, random
from datetime import date
from faker import Faker

today = date.today()  # get today's date
path = f"../Kafka/{today}"  # creates subfolder for journaled data per day

customer = {"id": "",  # customer journaled data
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


def get_info(journal: dict) -> dict:
    fake = Faker()
    types = ["Basic", "Standard", "Family Package", "Premium", "Xtreme Premium",
             "Supreme", "Pay-As-Go", "Fixed Access", "Enterprise", "Supreme Plus"]
    for key in journal.keys():
        match key:
            case "id":
                journal[key] = random.randint(1, 250)
            case "active":
                journal[key] = fake.pybool()
            case "subscription":
                journal[key] = random.choice(types)
            case "customer_first_name":
                journal[key] = fake.first_name()
            case "customer_last_name":
                journal[key] = fake.last_name()
            case "cost":
                journal[key] = random.randint(100, 2000)
            case "start_date":
                journal[key] = fake.date_this_decade().strftime('%Y-%m-%d')
            case "end_date":
                journal[key] = fake.future_date().strftime('%Y-%m-%d')
            case _:
                logger.error("Invalid keys!")
                exit(1)

    return journal


def generate():
    for n in range(1, 101):
        file = get_name(n)
        data = get_info(customer)
        with open(f"{path}/{file}", "w", encoding="utf-8") as outfile:
            json.dump(data, outfile, indent=4)


if __name__ == '__main__':
    if not os.path.exists(path):
        os.makedirs(path)
        generate()

    else:
        print("Path already exists and is a directory!")
        exit(1)
