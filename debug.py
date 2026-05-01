import bcrypt

stored_hash = b"$2a$06$UIruxYIWjutZ284pJAAwBuG9VouSjqRrGgtBHStzhTASxZMihtG7K"
password = "password123"

if bcrypt.checkpw(password.encode('utf-8'), stored_hash):
    print("Password matches!")
else:
    print("Password does not match!")
