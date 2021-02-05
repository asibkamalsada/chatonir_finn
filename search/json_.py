import json

if __name__ == '__main__':
    with open("json/fulltexts.json", "r") as file:
        json.load(file)