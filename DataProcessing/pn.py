import phonenumbers

# Монголын утасны дугаарууд
numbers = [
    "87450000",
    "87883434",
    "18001363",
    "18001890",
    "19001950",
    "70112345",
    "96117121",
    "80186785",
    "60240013"
]

print("Дугаар       | Зөв эсэх | Төрөл")
print("-" * 50)

for num in numbers:
    try:
        parsed = phonenumbers.parse(num, "MN")
        is_valid = phonenumbers.is_valid_number(parsed)
        number_type = phonenumbers.number_type(parsed)
        
        type_names = {
            0: "Суурин",
            1: "Утас", 
            -1: "Тодорхойгүй"
        }
        
        print(f"{num} | {is_valid:5} | {type_names.get(number_type, 'Тодорхойгүй')}")
    except Exception as e:
        print(f"{num} | Алдаа | {e}")
