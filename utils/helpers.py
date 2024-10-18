from datetime import datetime

def calculate_age(birthdate):
    try:
        today = datetime.today()
        age = today.year - birthdate.year
        if (today.month, today.day) < (birthdate.month, birthdate.day):
            age -= 1
        return age

    except:
        return 40
